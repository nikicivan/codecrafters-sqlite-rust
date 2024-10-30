use crate::{
    db::{
        encoding::{SerialType, SerialTypeDescription, Varint},
        sql::{
            ColumnDef, Condition, CreateIndexStatement, CreateTableStatement, SimpleCondition,
            SqlStatement, SqlValue,
        },
        types::{DatabaseEncoding, DatabaseHeader},
    },
    utils::utils::ReadFromBytes,
};
use anyhow::{anyhow, bail, Context, Result};
use std::{
    cmp::Ordering,
    collections::HashMap,
    fmt::Debug,
    ops::{Bound, RangeBounds},
    str::FromStr,
};
use strum::{EnumString, EnumTryAs};

#[derive(Debug, EnumString)]
pub enum ObjectType {
    #[strum(serialize = "table")]
    Table,
    #[strum(serialize = "index")]
    Index,
}

type RowId = u64;

#[derive(Debug)]
pub struct Row<'a> {
    pub row_id: RowId,
    data: HashMap<&'a str, SerialType>,
    primary_key_column: Option<&'a str>,
}

impl<'a> Row<'a> {
    pub fn new<T: IntoIterator<Item = &'a String>>(
        row_id: RowId,
        record: Record,
        columns: T,
        primary_key_column: Option<&'a str>,
    ) -> Self {
        let data = std::iter::zip(columns.into_iter(), record.0.into_iter())
            .map(|(column, value)| (column.as_str(), value))
            .collect();
        Self {
            row_id,
            data,
            primary_key_column,
        }
    }

    pub fn evaluate_condition(&self, condition: &Condition) -> bool {
        match condition {
            Condition::Simple(condition) => match condition {
                SimpleCondition::Equal(column, value) => {
                    self.get(column).map_or(false, |v| &v == value)
                }
            },
            Condition::And(left, right) => {
                self.evaluate_condition(left) && self.evaluate_condition(right)
            }
            Condition::Or(left, right) => {
                self.evaluate_condition(left) || self.evaluate_condition(right)
            }
        }
    }

    pub fn get(&self, column: &str) -> Option<SerialType> {
        let row_data = self.data.get(column);
        row_data.cloned().map(|val| {
            if val.is_null() && self.primary_key_column.map_or(false, |v| v == column) {
                SerialType::U64(self.row_id)
            } else {
                val
            }
        })
    }
}

pub fn sqlite_schema_spec() -> CreateTableStatement {
    SqlStatement::from_str(
        "CREATE TABLE sqlite_schema(\n\
            type text,\n\
            name text,\n\
            tbl_name text,\n\
            rootpage integer,\n\
            sql text\n\
        )",
    )
    .expect("Unreachable: failed to parse hardcoded sqlite_schema table creation SQL")
    .try_as_create_table()
    .expect("Unreachable: sqlite_schema table creation SQL was parsed as something other than a CREATE TABLE statement")
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

impl PageType {
    fn is_table(&self) -> bool {
        match self {
            Self::InteriorTable | Self::LeafTable => true,
            _ => false,
        }
    }

    fn is_interior(&self) -> bool {
        match self {
            Self::InteriorIndex | Self::InteriorTable => true,
            _ => false,
        }
    }
}

impl TryFrom<u8> for PageType {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self> {
        match value {
            2 => Ok(Self::InteriorIndex),
            5 => Ok(Self::InteriorTable),
            10 => Ok(Self::LeafIndex),
            13 => Ok(Self::LeafTable),
            value => bail!("Invalid page type: {}", value),
        }
    }
}

#[derive(Debug)]
pub struct PageHeader<'a> {
    page_type: PageType,
    #[allow(dead_code)]
    first_freeblock_offset: u16,
    #[allow(dead_code)]
    number_of_cells: u16,
    #[allow(dead_code)]
    start_of_cell_content_area: u32,
    #[allow(dead_code)]
    number_of_fragmented_free_bytes: u8,
    right_most_pointer: Option<u32>,
    cell_pointers: Vec<u16>,
    db_header: DatabaseHeader,
    buf: &'a [u8],
    page_number: u32,
}

#[derive(Debug)]
pub struct TablePageChain<'a> {
    pub cell_pointers: Vec<usize>,
    buf: &'a [u8],
    db_header: DatabaseHeader,
    pub column_defs: Vec<ColumnDef>,
    primary_key_column: Option<String>,
}

#[derive(Debug)]
pub struct IndexPageChain<'a> {
    cell_pointers: Vec<usize>,
    buf: &'a [u8],
    db_header: DatabaseHeader,
    pub spec: CreateIndexStatement,
}

impl<'a> TablePageChain<'a> {
    fn read(page_size: u16, header: PageHeader<'a>, column_defs: Vec<ColumnDef>) -> Result<Self> {
        if header.page_type.is_table() {
            let page_chain = BasePageChain::read(header)?;
            let cell_pointers = page_chain
                .0
                .iter()
                .flat_map(|header| {
                    header.cell_pointers.iter().map(|pointer| {
                        usize::from(*pointer)
                            + ((header.page_number as usize) - 1) * usize::from(page_size)
                    })
                })
                .collect();
            let first_page = &page_chain.0[0];
            Ok(Self {
                cell_pointers,
                buf: first_page.buf,
                db_header: first_page.db_header.clone(),
                primary_key_column: column_defs
                    .iter()
                    .find(|col| col.is_primary_key)
                    .map(|col| col.name.clone()),
                column_defs,
            })
        } else {
            bail!("Expected table page, got {:?}", header.page_type)
        }
    }

    fn find_cell(&self, idx: RowId) -> Result<usize, usize> {
        self.cell_pointers.binary_search_by(|cell_offset| {
            let cell = LeafTableCell::read_from_bytes(
                &self.buf,
                &mut usize::from(*cell_offset),
                &self.db_header.database_text_encoding,
            )
            .expect("Unreachable: failed to read cell");
            cell.row_id.0.cmp(&idx)
        })
    }

    fn read_cell(&self, idx: usize) -> Result<LeafTableCell> {
        let cell_offset = &self.cell_pointers[idx];
        LeafTableCell::read_from_bytes(
            &self.buf,
            &mut usize::from(*cell_offset),
            &self.db_header.database_text_encoding,
        )
        .with_context(|| {
            format!(
                "TablePageChain::read_cell: Failed to read cell at offset {}",
                cell_offset
            )
        })
    }

    fn read_cell_range<R: RangeBounds<usize> + Debug>(
        &self,
        range: R,
    ) -> Result<Vec<LeafTableCell>> {
        // eprintln!("Reading cell range: {:?}", self.cell_pointers);
        let start_index = match range.start_bound() {
            Bound::Included(&start) => start,
            Bound::Excluded(&start) => start + 1,
            Bound::Unbounded => 0,
        };

        let end_index = match range.end_bound() {
            Bound::Included(&end) => end + 1,
            Bound::Excluded(&end) => end,
            Bound::Unbounded => self.cell_pointers.len(),
        };

        let concrete_range = start_index..end_index;
        let concrete_range_copy = concrete_range.clone();

        concrete_range
            .map(|idx| {
                self.read_cell(idx).with_context(|| {
                    format!(
                        "TablePageChain::read_cell_range: Failed to read cell at index {} while reading range {concrete_range_copy:?}",
                        idx
                    )
                })
            })
            .collect()
    }

    pub fn read_row_range<R: RangeBounds<usize> + Debug>(&self, range: R) -> Result<Vec<Row>> {
        self.read_cell_range(range)?
            .into_iter()
            .map(|cell| {
                Ok(Row::new(
                    cell.row_id.0,
                    cell.record,
                    self.column_defs.iter().map(|col| &col.name),
                    self.primary_key_column.as_ref().map(|s| s.as_str()),
                ))
            })
            .collect()
    }

    fn read_row(&self, idx: usize) -> Result<Row> {
        let cell = self.read_cell(idx)?;
        Ok(Row::new(
            cell.row_id.0,
            cell.record,
            self.column_defs.iter().map(|col| &col.name),
            self.primary_key_column.as_ref().map(|s| s.as_str()),
        ))
    }

    pub fn find_row(&self, row_id: RowId) -> Option<Result<Row>> {
        let cell_index = self.find_cell(row_id).ok()?;
        Some(self.read_row(cell_index))
    }
}

impl PartialOrd<SqlValue> for SerialType {
    fn partial_cmp(&self, other: &SqlValue) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (SerialType::Null, _) => Some(Ordering::Less),
            (SerialType::U8(a), SqlValue::Integer(b)) => i128::from(*a).partial_cmp(b),
            (SerialType::U16(a), SqlValue::Integer(b)) => i128::from(*a).partial_cmp(b),
            (SerialType::U24(a), SqlValue::Integer(b)) => i128::from(*a).partial_cmp(b),
            (SerialType::U32(a), SqlValue::Integer(b)) => i128::from(*a).partial_cmp(b),
            (SerialType::U48(a), SqlValue::Integer(b)) => i128::from(*a).partial_cmp(b),
            (SerialType::U64(a), SqlValue::Integer(b)) => i128::from(*a).partial_cmp(b),
            (SerialType::Text(a), SqlValue::String(b)) => a.partial_cmp(b),
            (_, _) => None,
        }
    }
}

impl<'a> IndexPageChain<'a> {
    fn read(page_size: u16, header: PageHeader<'a>, spec: CreateIndexStatement) -> Result<Self> {
        if header.page_type.is_table() {
            bail!("Expected index page, got {:?}", header.page_type)
        } else {
            let page_chain = BasePageChain::read(header)?;
            let cell_pointers = page_chain
                .0
                .iter()
                .flat_map(|header| {
                    header.cell_pointers.iter().map(|pointer| {
                        usize::from(*pointer)
                            + ((header.page_number as usize) - 1) * usize::from(page_size)
                    })
                })
                .collect();
            let first_page = &page_chain.0[0];
            Ok(Self {
                cell_pointers,
                buf: first_page.buf,
                db_header: first_page.db_header.clone(),
                spec,
            })
        }
    }

    pub fn evaluate_condition(&self, condition: &Condition) -> Result<Vec<RowId>> {
        let (cond_col_name, cond_value) = match condition {
            Condition::Simple(SimpleCondition::Equal(column_name, value)) => (column_name, value),
            _ => bail!("Expected simple equal condition, got {:?}", condition),
        };
        let col_index = self
            .spec
            .columns
            .iter()
            .position(|col| col == cond_col_name)
            .ok_or_else(|| anyhow!("Column {} not found", cond_col_name))?;

        let match_idx = self.cell_pointers.binary_search_by(|cell_offset| {
            let record = IndexRecord::read_from_bytes(
                &self.buf,
                &mut usize::from(*cell_offset),
                &self.db_header.database_text_encoding,
            )
            .expect("Unreachable: failed to read cell");
            let value = &record.record.0[col_index];
            value.partial_cmp(cond_value).expect(format!("Unreachable: comparing incompatible values: {value:?} (DB) and {cond_value:?} (condition)").as_str())
        });

        let match_idx = if let Ok(match_idx) = match_idx {
            match_idx
        } else {
            return Ok(vec![]);
        };

        let start = self.cell_pointers[..match_idx]
            .iter()
            .rev()
            .position(|cell_offset| {
                let record = IndexRecord::read_from_bytes(
                    &self.buf,
                    &mut usize::from(*cell_offset),
                    &self.db_header.database_text_encoding,
                )
                .expect("Unreachable: failed to read cell");
                let value = &record.record.0[col_index];
                value.ne(cond_value)
            })
            .map(|idx| match_idx - idx)
            .unwrap_or(0);
        let end = self.cell_pointers[match_idx..]
            .iter()
            .position(|cell_offset| {
                let record = IndexRecord::read_from_bytes(
                    &self.buf,
                    &mut usize::from(*cell_offset),
                    &self.db_header.database_text_encoding,
                )
                .expect("Unreachable: failed to read cell");
                let value = &record.record.0[col_index];
                value.ne(cond_value)
            })
            .map(|v| match_idx + v);

        let end = if let Some(end) = end {
            end
        } else {
            return Ok(vec![]);
        };

        let row_ids = self.cell_pointers[start..end]
            .iter()
            .map(|cell_offset| {
                let record = IndexRecord::read_from_bytes(
                    &self.buf,
                    &mut usize::from(*cell_offset),
                    &self.db_header.database_text_encoding,
                )?;
                Ok(record.row_id)
            })
            .collect::<Result<Vec<_>>>()?;

        eprintln!(
            "match_idx: {match_idx}\nstart: {start}\nend: {end}\nrow_ids around results: {:?}",
            self.cell_pointers
                [start.checked_sub(10).unwrap_or(start)..(end + 10).min(self.cell_pointers.len())]
                .iter()
                .map(|cell_offset| IndexRecord::read_from_bytes(
                    &self.buf,
                    &mut usize::from(*cell_offset),
                    &self.db_header.database_text_encoding,
                ))
                .collect::<Result<Vec<_>>>()
        );
        Ok(row_ids)
    }
}

#[derive(Debug, EnumTryAs)]
pub enum PageChain<'a> {
    Table(TablePageChain<'a>),
    Index(IndexPageChain<'a>),
}

impl<'a> PageChain<'a> {
    pub fn read_from_bytes(
        buf: &'a [u8],
        page_number: u32,
        db_header: &DatabaseHeader,
        sql: SqlStatement,
    ) -> Result<Self> {
        let header = PageHeader::read_from_bytes(buf, page_number, db_header)?;
        Self::read(db_header.page_size, header, sql)
    }

    fn read(page_size: u16, header: PageHeader<'a>, sql: SqlStatement) -> Result<Self> {
        if header.page_type.is_table() {
            let column_defs = if let SqlStatement::CreateTable(sql) = sql {
                sql.columns
            } else {
                bail!(
                    "Expected CreateTableStatement for page type {:?}, got {sql:?}",
                    header.page_type
                )
            };
            Ok(Self::Table(TablePageChain::read(
                page_size,
                header,
                column_defs,
            )?))
        } else {
            let sql = if let SqlStatement::CreateIndex(sql) = sql {
                sql
            } else {
                bail!(
                    "Expected CreateIndexStatement for page type {:?}, got {sql:?}",
                    header.page_type
                )
            };
            Ok(Self::Index(IndexPageChain::read(page_size, header, sql)?))
        }
    }
}

#[derive(Debug)]
struct BasePageChain<'a>(Vec<PageHeader<'a>>);

impl<'a> BasePageChain<'a> {
    fn read(header: PageHeader<'a>) -> Result<Self> {
        Ok(Self(Self::read_pages(header)?))
    }
    fn read_pages(header: PageHeader<'a>) -> Result<Vec<PageHeader<'a>>> {
        let base_offset = usize::try_from(header.page_number - 1).with_context(|| {
            format!(
                "Failed to convert page number {} to usize",
                header.page_number
            )
        })? * usize::from(header.db_header.page_size);
        Ok(if header.page_type.is_interior() {
            header
                .cell_pointers
                .iter()
                .map(|&offset| {
                    let mut index = base_offset + usize::from(offset);
                    u32::read_from_bytes(&header.buf, &mut index)
                })
                .chain(std::iter::once(header.right_most_pointer.ok_or_else(
                    || {
                        anyhow!(
                            "Expected right most pointer for {:?} page",
                            header.page_type
                        )
                    },
                )))
                .map(|page_number| -> Result<_> {
                    Self::read_pages(PageHeader::read_from_bytes(
                        &header.buf,
                        page_number?,
                        &header.db_header,
                    )?)
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .collect()
        } else {
            vec![header]
        })
    }
}

impl<'a> PageHeader<'a> {
    fn read_from_bytes(
        buf: &'a [u8],
        page_number: u32,
        db_header: &DatabaseHeader,
    ) -> Result<Self> {
        let index = ((page_number - 1) * u32::from(db_header.page_size)) as usize;
        let index = &mut if page_number == 1 { index + 100 } else { index };
        let page_type = PageType::try_from(u8::read_from_bytes(buf, index)?)
            .map_err(|got| anyhow!("Invalid page type: {got}"))?;
        let first_freeblock_offset = ReadFromBytes::read_from_bytes(buf, index)?;
        let number_of_cells = ReadFromBytes::read_from_bytes(buf, index)?;
        let start_of_cell_content_area: u16 = ReadFromBytes::read_from_bytes(buf, index)?;
        let start_of_cell_content_area = if start_of_cell_content_area == 0 {
            u32::from(u16::MAX) + 1
        } else {
            u32::from(start_of_cell_content_area)
        };
        let number_of_fragmented_free_bytes = ReadFromBytes::read_from_bytes(buf, index)?;
        let right_most_pointer = match page_type {
            PageType::InteriorIndex | PageType::InteriorTable => {
                Some(ReadFromBytes::read_from_bytes(buf, index)?)
            }
            _ => None,
        };

        let cell_pointers = (0..number_of_cells)
            .map(|_| u16::read_from_bytes(buf, index))
            .collect::<Result<Vec<_>>>()
            .with_context(|| "Failed to read cell pointers")?;

        Ok(PageHeader {
            page_type,
            first_freeblock_offset,
            number_of_cells,
            start_of_cell_content_area,
            number_of_fragmented_free_bytes,
            right_most_pointer,
            cell_pointers,
            buf,
            db_header: db_header.clone(),
            page_number,
        })
    }
}

#[derive(Debug)]
pub struct Record(pub Vec<SerialType>);

impl Record {
    fn read_from_bytes(
        bytes: &[u8],
        index: &mut usize,
        text_encoding: &DatabaseEncoding,
        payload_size: usize,
    ) -> Result<Self> {
        let initial_index = *index;
        let header_size: usize = Varint::read_from_bytes(bytes, index)
            .with_context(|| "Record::read_from_bytes: Failed to read record header size")?
            .0
            .try_into()
            .with_context(|| {
                "Record::read_from_bytes: Failed to convert record header size to usize"
            })?;

        #[cfg(debug_assertions)]
        eprintln!("header_size: {}", header_size);

        let serial_type_descriptions = std::iter::from_fn(|| {
            let current_index = *index - initial_index;
            if current_index < header_size {
                Some(
                    Varint::read_from_bytes(bytes, index).and_then(SerialTypeDescription::try_from).with_context(
                        || format!("Record::read_from_bytes: Failed to read serial type description ({current_index}/{header_size})"),
                    ),
                )
            } else {
                None
            }
        })
        .collect::<Result<Vec<_>>>()?;

        #[cfg(debug_assertions)]
        eprintln!("serial_type_descriptions: {:?}", serial_type_descriptions);

        if *index - initial_index != header_size {
            bail!(
                "Expected record header size to be {}, got {}",
                header_size,
                *index - initial_index
            );
        }

        let cols = serial_type_descriptions
            .into_iter()
            .map(|desc| {
                desc.read_from_bytes(bytes, index, text_encoding)
                    .with_context(|| {
                        format!(
                            "Record::read_from_bytes: Failed to read record column from {desc:?}"
                        )
                    })
            })
            .collect::<Result<_, _>>()?;

        if *index - initial_index != payload_size {
            bail!(
                "Record::read_from_bytes: Expected record payload size to be {}, got {}",
                payload_size,
                *index - initial_index
            );
        }
        Ok(Record(cols))
    }
}

#[derive(Debug)]
pub struct IndexRecord {
    row_id: u64,
    record: Record,
}

impl IndexRecord {
    fn read_from_bytes(
        bytes: &[u8],
        index: &mut usize,
        text_encoding: &DatabaseEncoding,
    ) -> Result<Self> {
        let payload_size = Varint::read_from_bytes(bytes, index)
            .with_context(|| "Reading InteriorIndex: Failed to read payload size")?
            .0;

        let payload_size = payload_size.try_into().with_context(|| {
            format!(
                "IndexRecord::read_from_bytes: Failed to convert payload size {} to usize",
                payload_size
            )
        })?;

        let mut record = Record::read_from_bytes(bytes, index, text_encoding, payload_size)
            .with_context(|| "IndexRecord::read_from_bytes: Failed to read record")?;

        let row_id = record
            .0
            .pop()
            .ok_or_else(|| anyhow!("IndexRecord::read_from_bytes: Missing row ID"))?
            .try_into()
            .with_context(|| {
                format!("IndexRecord::read_from_bytes: Failed to convert row ID to u64")
            })?;
        Ok(IndexRecord { record, row_id })
    }
}

#[derive(Debug)]
pub struct LeafTableCell {
    row_id: Varint,
    pub record: Record,
}

impl LeafTableCell {
    fn read_from_bytes(
        bytes: &[u8],
        index: &mut usize,
        text_encoding: &DatabaseEncoding,
    ) -> Result<Self> {
        let payload_size: usize = Varint::read_from_bytes(bytes, index)
            .with_context(|| "LeafTableCell::read_from_bytes: Failed to read payload size")?
            .0
            .try_into()
            .with_context(|| {
                format!("LeafTableCell::read_from_bytes: Failed to convert payload size to usize")
            })?;

        #[cfg(debug_assertions)]
        eprintln!("cell_size: {}", payload_size);

        let row_id = Varint::read_from_bytes(bytes, index)
            .with_context(|| "LeafTableCell::read_from_bytes: Failed to read row ID")?;

        #[cfg(debug_assertions)]
        eprintln!("row_id: {:?}", row_id);

        let record = Record::read_from_bytes(bytes, index, text_encoding, payload_size)
            .with_context(|| "LeafTableCell::read_from_bytes: Failed to read record")?;
        #[cfg(debug_assertions)]
        eprintln!("record: {:?}", record);

        Ok(LeafTableCell { row_id, record })
    }
}
