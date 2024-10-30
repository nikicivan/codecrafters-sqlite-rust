use crate::{
    db::sql::{Condition, Selection, SimpleCondition, SqlStatement, SqlValue},
    utils::utils::ReadFromBytes,
};
use anyhow::{anyhow, bail, Context, Result};
use std::{collections::HashMap, str::FromStr};
use strum::EnumIs;
use thiserror::Error;

use super::sql::ColumnDef;

const MAGIC_STRING_LEN: usize = 16;
const MAGIC_STRING: [u8; MAGIC_STRING_LEN] = *b"SQLite format 3\0";
#[derive(Debug)]
enum FileFormatVersion {
    Legacy,
    WAL,
}

impl TryFrom<u8> for FileFormatVersion {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        match value {
            1 => Ok(Self::Legacy),
            2 => Ok(Self::WAL),
            value => Err(value),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum DatabaseEncoding {
    Utf8,
    Utf16Le,
    Utf16Be,
}
impl TryFrom<u32> for DatabaseEncoding {
    type Error = u32;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Utf8),
            2 => Ok(Self::Utf16Le),
            3 => Ok(Self::Utf16Be),
            value => Err(value),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct DatabaseHeader {
    // skip: 16 bytes. must be MAGIC_STRING
    // size: 2 bytes
    pub page_size: u16,
    // size: 1 byte
    file_format_write_version: FileFormatVersion,
    // size: 1 byte
    file_format_read_version: FileFormatVersion,
    // size: 1 byte
    reserved_space_per_page: u8,
    // size: 1 byte, must be 64
    max_embedded_payload_fraction: u8,
    // size: 1 byte, must be 32
    min_embedded_payload_fraction: u8,
    // size: 1 byte
    leaf_payload_fraction: u8,
    // size: 4 bytes
    file_change_counter: u32,
    // size: 4 bytes
    database_size_in_pages: u32,
    // size: 4 bytes
    first_freelist_trunk_page: u32,
    // size: 4 bytes
    total_freelist_pages: u32,
    // size: 4 bytes
    schema_cookie: u32,
    // size: 4 bytes. must be 1, 2, 3, or 4
    schema_format_number: u32,
    // size: 4 bytes
    default_page_cache_size: u32,
    // size: 4 bytes. should be 0 unless in auto- or incremental-vacuum modes
    largest_btree_page_number: u32,
    // size: 4 bytes
    pub database_text_encoding: DatabaseEncoding,
    // size: 4 bytes
    user_version: u32,
    // size: 4 bytes
    is_incremental_vacuum: bool,
    // size: 4 bytes
    application_id: u32,
    // skip: 20 bytes. must be 0
    // size: 4 bytes
    version_valid_for: u32,
    // size: 4 bytes
    sqlite_version_number: u32,
}

impl ReadFromBytes for DatabaseHeader {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        {
            let magic_string: [u8; MAGIC_STRING_LEN] =
                ReadFromBytes::read_from_bytes(bytes, index)?;
            if magic_string.len() < MAGIC_STRING.len() {
                Err(HeaderParseError::UnexpectedEndOfInput)?;
            } else if magic_string != MAGIC_STRING {
                Err(HeaderParseError::InvalidMagicString(
                    String::from_utf8_lossy(&magic_string).to_string(),
                ))?;
            }
        }
        let page_size = ReadFromBytes::read_from_bytes(bytes, index)?;

        let file_format_write_version =
            FileFormatVersion::try_from(u8::read_from_bytes(bytes, index)?)
                .map_err(HeaderParseError::InvalidFileFormatReadVersion)?;

        let file_format_read_version =
            FileFormatVersion::try_from(u8::read_from_bytes(bytes, index)?)
                .map_err(HeaderParseError::InvalidFileFormatWriteVersion)?;

        let reserved_space_per_page = ReadFromBytes::read_from_bytes(bytes, index)?;

        let max_embedded_payload_fraction = ReadFromBytes::read_from_bytes(bytes, index)?;

        let min_embedded_payload_fraction = ReadFromBytes::read_from_bytes(bytes, index)?;

        let leaf_payload_fraction = ReadFromBytes::read_from_bytes(bytes, index)?;

        let file_change_counter = ReadFromBytes::read_from_bytes(bytes, index)?;

        let database_size_in_pages = ReadFromBytes::read_from_bytes(bytes, index)?;

        let first_freelist_trunk_page = ReadFromBytes::read_from_bytes(bytes, index)?;

        let total_freelist_pages = ReadFromBytes::read_from_bytes(bytes, index)?;

        let schema_cookie = ReadFromBytes::read_from_bytes(bytes, index)?;

        let schema_format_number = ReadFromBytes::read_from_bytes(bytes, index)?;

        let default_page_cache_size = ReadFromBytes::read_from_bytes(bytes, index)?;

        let largest_btree_page_number = ReadFromBytes::read_from_bytes(bytes, index)?;

        let database_text_encoding =
            DatabaseEncoding::try_from(u32::read_from_bytes(bytes, index)?)
                .map_err(HeaderParseError::InvalidDatabaseTextEncoding)?;

        let user_version = ReadFromBytes::read_from_bytes(bytes, index)?;

        let is_incremental_vacuum = u32::read_from_bytes(bytes, index)? != 0;

        let application_id = ReadFromBytes::read_from_bytes(bytes, index)?;

        // skip 20 reserved bytes
        for _ in 0..20 {
            if u8::read_from_bytes(bytes, index)? != 0 {
                Err(HeaderParseError::InvalidReservedBytes)?;
            }
        }

        let version_valid_for = ReadFromBytes::read_from_bytes(bytes, index)?;

        let sqlite_version_number = ReadFromBytes::read_from_bytes(bytes, index)?;
        Ok(DatabaseHeader {
            page_size,
            file_format_write_version,
            file_format_read_version,
            reserved_space_per_page,
            max_embedded_payload_fraction,
            min_embedded_payload_fraction,
            leaf_payload_fraction,
            file_change_counter,
            database_size_in_pages,
            first_freelist_trunk_page,
            total_freelist_pages,
            schema_cookie,
            schema_format_number,
            default_page_cache_size,
            largest_btree_page_number,
            database_text_encoding,
            user_version,
            is_incremental_vacuum,
            application_id,
            version_valid_for,
            sqlite_version_number,
        })
    }
}

#[derive(Debug, Error)]
enum HeaderParseError {
    #[error("Unexpected end of input")]
    UnexpectedEndOfInput,
    #[error("Invalid magic string")]
    InvalidMagicString(String),
    #[error("Invalid file format read version: {0}")]
    InvalidFileFormatReadVersion(u8),
    #[error("Invalid file format write version: {0}")]
    InvalidFileFormatWriteVersion(u8),
    #[error("Invalid database text encoding: {0}")]
    InvalidDatabaseTextEncoding(u32),
    #[error("Invalid reserved bytes")]
    InvalidReservedBytes,
}

#[derive(Debug, PartialEq, Eq)]
enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
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
pub struct LeafTable {
    pub cells: Vec<LeafTableCell>,
}

#[derive(Debug)]
pub struct InteriorTable {
    pub pages: Vec<Page>,
}

#[derive(Debug)]
pub enum Page {
    InteriorIndex,
    InteriorTable(InteriorTable),
    LeafIndex,
    LeafTable(LeafTable),
}

impl Page {
    fn leaf_table_cells(self) -> Vec<LeafTableCell> {
        match self {
            Self::LeafTable(LeafTable { cells, .. }) => cells.into_iter().collect(),
            Self::InteriorTable(InteriorTable { pages }) => pages
                .into_iter()
                .flat_map(|page| page.leaf_table_cells())
                .collect(),
            _ => vec![],
        }
    }

    pub fn read_from_bytes(
        bytes: &[u8],
        index: &mut usize,
        db_header: &DatabaseHeader,
        base_offset: usize,
    ) -> Result<Self> {
        let page_header = PageHeader::read_from_bytes(bytes, index)?;

        #[cfg(debug_assertions)]
        eprintln!("page_header: {page_header:?}");

        let cell_pointers = (0..page_header.number_of_cells)
            .map(|_| u16::read_from_bytes(bytes, index))
            .collect::<Result<Vec<_>>>()
            .with_context(|| "Failed to read cell pointers")?;

        match page_header.page_type {
            PageType::LeafTable => {
                #[cfg(debug_assertions)]
                eprintln!("cell_pointers: {cell_pointers:?}");

                let cells = cell_pointers
                    .iter()
                    .map(|&offset| {
                        let mut index = base_offset + usize::from(offset);

                        LeafTableCell::read_from_bytes(
                            bytes,
                            &mut index,
                            &db_header.database_text_encoding,
                        )
                    })
                    .collect::<Result<Vec<_>>>()
                    .with_context(|| "Failed to read cells")?;

                Ok(Self::LeafTable(LeafTable { cells }))
            }
            PageType::InteriorTable => {
                let pages = cell_pointers
                    .iter()
                    .map(|&offset| {
                        let mut index = base_offset + usize::from(offset);
                        u32::read_from_bytes(bytes, &mut index)
                    })
                    .chain(std::iter::once(page_header.right_most_pointer.ok_or_else(
                        || anyhow!("Expected right most pointer for InteriorTable page"),
                    )))
                    .map(|page_number| {
                        let base_offset =
                            ((page_number? - 1) as usize) * usize::from(db_header.page_size);
                        Page::read_from_bytes(
                            bytes,
                            &mut base_offset.clone(),
                            db_header,
                            base_offset,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::InteriorTable(InteriorTable { pages }))
            }
            PageType::InteriorIndex => Ok(Self::InteriorIndex),
            PageType::LeafIndex => Ok(Self::LeafIndex),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct PageHeader {
    page_type: PageType,
    first_freeblock_offset: u16,
    pub number_of_cells: u16,
    start_of_cell_content_area: u32,
    number_of_fragmented_free_bytes: u8,
    right_most_pointer: Option<u32>,
}

impl ReadFromBytes for PageHeader {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let page_type = PageType::try_from(u8::read_from_bytes(bytes, index)?)
            .map_err(|got| anyhow!("Invalid page type: {got}"))?;
        let first_freeblock_offset = ReadFromBytes::read_from_bytes(bytes, index)?;
        let number_of_cells = ReadFromBytes::read_from_bytes(bytes, index)?;
        let start_of_cell_content_area: u16 = ReadFromBytes::read_from_bytes(bytes, index)?;
        let start_of_cell_content_area = if start_of_cell_content_area == 0 {
            u32::from(u16::MAX) + 1
        } else {
            u32::from(start_of_cell_content_area)
        };
        let number_of_fragmented_free_bytes = ReadFromBytes::read_from_bytes(bytes, index)?;
        let right_most_pointer = match page_type {
            PageType::InteriorIndex | PageType::InteriorTable => {
                Some(ReadFromBytes::read_from_bytes(bytes, index)?)
            }
            _ => None,
        };
        Ok(PageHeader {
            page_type,
            first_freeblock_offset,
            number_of_cells,
            start_of_cell_content_area,
            number_of_fragmented_free_bytes,
            right_most_pointer,
        })
    }
}

#[derive(Debug)]
struct Varint(u64);
impl ReadFromBytes for Varint {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        type Num = u64;
        let mut value: Num = 0;
        for i in 0..9 {
            let byte = u8::read_from_bytes(bytes, index)?;
            if i == 8 {
                value = value << 8 | Num::from(byte);
            } else {
                value = value << 7 | Num::from(byte & 0b0111_1111);
            }
            if byte & 0b1000_0000 == 0 {
                break;
            }
        }
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, EnumIs)]
pub enum SerialType {
    Null,
    U8(u8),
    U16(u16),
    U24(u32),
    U32(u32),
    U48(u64),
    U64(u64),
    F64(f64),
    I0,
    I1,
    Blob(Vec<u8>),
    Text(String),
}

impl std::fmt::Display for SerialType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerialType::Null => write!(f, "NULL"),
            SerialType::U8(value) => write!(f, "{}", value),
            SerialType::U16(value) => write!(f, "{}", value),
            SerialType::U24(value) => write!(f, "{}", value),
            SerialType::U32(value) => write!(f, "{}", value),
            SerialType::U48(value) => write!(f, "{}", value),
            SerialType::U64(value) => write!(f, "{}", value),
            SerialType::F64(value) => write!(f, "{}", value),
            SerialType::I0 => write!(f, "I0"),
            SerialType::I1 => write!(f, "I1"),
            SerialType::Blob(value) => write!(f, "{:?}", value),
            SerialType::Text(value) => write!(f, "{}", value),
        }
    }
}

#[derive(Debug)]
enum SerialTypeDescription {
    Null,
    U8,
    U16,
    U24,
    U32,
    U48,
    U64,
    F64,
    I0,
    I1,
    Blob(usize),
    Text(usize),
}

impl TryFrom<Varint> for SerialTypeDescription {
    type Error = anyhow::Error;
    fn try_from(value: Varint) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            0 => Ok(Self::Null),
            1 => Ok(Self::U8),
            2 => Ok(Self::U16),
            3 => Ok(Self::U24),
            4 => Ok(Self::U32),
            5 => Ok(Self::U48),
            6 => Ok(Self::U64),
            7 => Ok(Self::F64),
            8 => Ok(Self::I0),
            9 => Ok(Self::I1),
            10 | 11 => Err(anyhow!("Invalid serial type: {}", value.0)),
            value if value % 2 == 0 => {
                Ok(Self::Blob(((value - 12) / 2).try_into().with_context(
                    || format!("Failed to convert blob size {} to usize", value),
                )?))
            }
            value => Ok(Self::Text(((value - 13) / 2).try_into().with_context(
                || format!("Failed to convert text size {} to usize", value),
            )?)),
        }
    }
}

impl SerialTypeDescription {
    fn read_from_bytes(
        &self,
        bytes: &[u8],
        index: &mut usize,
        text_encoding: &DatabaseEncoding,
    ) -> Result<SerialType> {
        match self {
            Self::Null => Ok(SerialType::Null),
            Self::U8 => Ok(SerialType::U8(u8::read_from_bytes(bytes, index)?)),
            Self::U16 => Ok(SerialType::U16(u16::read_from_bytes(bytes, index)?)),
            Self::U24 => Ok(SerialType::U24({
                let arr: [u8; 3] = ReadFromBytes::read_from_bytes(bytes, index)?;
                u32::from_be_bytes([0, arr[0], arr[1], arr[2]])
            })),
            Self::U32 => Ok(SerialType::U32(u32::read_from_bytes(bytes, index)?)),
            Self::U48 => Ok(SerialType::U48({
                let arr: [u8; 6] = ReadFromBytes::read_from_bytes(bytes, index)?;
                u64::from_be_bytes([0, 0, arr[0], arr[1], arr[2], arr[3], arr[4], arr[5]])
            })),
            Self::U64 => Ok(SerialType::U64(u64::read_from_bytes(bytes, index)?)),
            Self::F64 => Ok(SerialType::F64(f64::from_bits(u64::read_from_bytes(
                bytes, index,
            )?))),
            Self::I0 => Ok(SerialType::I0),
            Self::I1 => Ok(SerialType::I1),
            Self::Blob(size) => {
                let blob = (0..*size)
                    .map(|_| u8::read_from_bytes(bytes, index))
                    .collect::<Result<Vec<_>>>()
                    .with_context(|| "Failed to read blob")?;
                Ok(SerialType::Blob(blob))
            }
            Self::Text(size) => {
                if text_encoding != &DatabaseEncoding::Utf8 && size % 2 != 0 {
                    bail!(
                        "Invalid text size for UTF-16 encoding. Expected an even number, got {}",
                        size
                    );
                }
                let bytes = (0..*size)
                    .map(|_| u8::read_from_bytes(bytes, index))
                    .collect::<Result<Vec<_>>>()
                    .with_context(|| "Failed to read text bytes")?;
                let text = match text_encoding {
                    DatabaseEncoding::Utf8 => {
                        String::from_utf8(bytes).with_context(|| "Invalid UTF-8")?
                    }
                    DatabaseEncoding::Utf16Le => String::from_utf16(
                        &bytes
                            .chunks_exact(2)
                            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                            .collect::<Vec<_>>(),
                    )
                    .with_context(|| "Invalid UTF-16")?,
                    DatabaseEncoding::Utf16Be => String::from_utf16(
                        &bytes
                            .chunks_exact(2)
                            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
                            .collect::<Vec<_>>(),
                    )
                    .with_context(|| "Invalid UTF-16")?,
                };
                Ok(SerialType::Text(text))
            }
        }
    }
}

#[derive(Debug)]
pub struct Record(pub Vec<SerialType>);

impl Record {
    fn read_from_bytes(
        bytes: &[u8],
        index: &mut usize,
        text_encoding: &DatabaseEncoding,
    ) -> Result<Self> {
        let initial_index = *index;
        let header_size: usize = Varint::read_from_bytes(bytes, index)?
            .0
            .try_into()
            .with_context(|| format!("Failed to convert record header size to usize"))?;
        #[cfg(debug_assertions)]
        eprintln!("header_size: {}", header_size);
        let serial_type_descriptions = std::iter::from_fn(|| {
            if *index - initial_index < header_size {
                Some(
                    Varint::read_from_bytes(bytes, index).and_then(SerialTypeDescription::try_from),
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
                    .with_context(|| "Failed to read record column")
            })
            .collect::<Result<_, _>>()?;
        Ok(Record(cols))
    }
}

#[derive(Debug)]
pub struct LeafTableCell {
    #[allow(dead_code)]
    row_id: Varint,
    pub record: Record,
}

impl LeafTableCell {
    fn read_from_bytes(
        bytes: &[u8],
        index: &mut usize,
        text_encoding: &DatabaseEncoding,
    ) -> Result<Self> {
        let size: usize = Varint::read_from_bytes(bytes, index)?
            .0
            .try_into()
            .with_context(|| format!("Failed to convert cell size to usize"))?;

        #[cfg(debug_assertions)]
        eprintln!("cell_size: {}", size);

        let row_id = Varint::read_from_bytes(bytes, index)?;

        #[cfg(debug_assertions)]
        eprintln!("row_id: {:?}", row_id);

        let initial_index = *index;
        let record = Record::read_from_bytes(bytes, index, text_encoding)?;

        #[cfg(debug_assertions)]
        eprintln!("record: {:?}", record);

        if *index - initial_index != size {
            bail!(
                "Expected cell payload size to be {}, got {}",
                size,
                *index - initial_index
            );
        }
        Ok(LeafTableCell { row_id, record })
    }
}

#[derive(Debug)]
pub struct Row {
    row_id: u64,
    data: HashMap<String, SerialType>,
    primary_key_column: Option<String>,
}

impl Row {
    fn get(&self, column: &str) -> Option<SerialType> {
        let row_data = self.data.get(column);
        row_data.cloned().map(|val| {
            if val.is_null()
                && self
                    .primary_key_column
                    .as_ref()
                    .map_or(false, |v| v == column)
            {
                SerialType::U64(self.row_id)
            } else {
                val
            }
        })
    }
}

#[derive(Debug)]
pub struct TableMetadata {
    entry_type: String,
    name: String,
    table_name: String,
    root_page: u8,
    sql: String,
}

impl TableMetadata {
    fn sqlite_schema_metadata() -> Self {
        Self {
            entry_type: "table".to_string(),
            name: "sqlite_schema".to_string(),
            table_name: "sqlite_schema".to_string(),
            root_page: 1,
            sql: "CREATE TABLE sqlite_schema(\n\
                type text,\n\
                name text,\n\
                tbl_name text,\n\
                rootpage integer,\n\
                sql text\n\
            )"
            .to_string(),
        }
    }

    fn from_sqlite_schema(row: &Row) -> Result<Self> {
        let entry_type = match row
            .get("type")
            .ok_or_else(|| anyhow!("Missing type column"))?
        {
            SerialType::Text(name) => name,
            _ => bail!("Expected type to be a text"),
        }
        .clone();

        let name = match row
            .get("name")
            .ok_or_else(|| anyhow!("Missing name column"))?
        {
            SerialType::Text(name) => name,
            _ => bail!("Expected table name to be a text"),
        }
        .clone();

        let table_name = match row
            .get("tbl_name")
            .ok_or_else(|| anyhow!("Missing tbl_name column"))?
        {
            SerialType::Text(table_name) => table_name,
            _ => bail!("Expected table name to be a text"),
        }
        .clone();

        let root_page = match row
            .get("rootpage")
            .ok_or_else(|| anyhow!("Missing rootpage column"))?
        {
            SerialType::U8(root_page) => root_page,
            _ => bail!("Expected root page to be a u8"),
        }
        .clone();

        let sql = match row
            .get("sql")
            .ok_or_else(|| anyhow!("Missing sql column"))?
        {
            SerialType::Text(sql) => sql,
            _ => bail!("Expected SQL to be a text"),
        }
        .clone()
        .trim()
        .to_string();

        Ok(TableMetadata {
            entry_type,
            name,
            table_name,
            root_page,
            sql,
        })
    }
}

#[derive(Debug)]
pub struct TableData {
    #[allow(dead_code)]
    name: String,
    pub table_name: String,
    #[allow(dead_code)]
    pub root_page: u8,
    #[allow(dead_code)]
    sql: String,
    pub data: Vec<Row>,
    pub columns: ColumnDefMap,
}

type ColumnDefMap = HashMap<String, ColumnDef>;

impl TableData {
    pub fn read(metadata: TableMetadata, db_header: &DatabaseHeader, bytes: &[u8]) -> Result<Self> {
        let TableMetadata {
            entry_type,
            name,
            table_name,
            root_page,
            sql,
        } = metadata;

        if entry_type != "table" {
            panic!("Expected entry type to be \"table\", got {entry_type:?}");
        }

        let columns = if let SqlStatement::CreateTable(create_table) = SqlStatement::from_str(&sql)?
        {
            create_table.columns
        } else {
            bail!("Expected SQL to be a CREATE TABLE statement");
        };

        let primary_key_column = columns
            .iter()
            .find(|column| column.is_primary_key)
            .map(|column| column.name.clone());

        let base_offset = usize::from(root_page - 1) * usize::from(db_header.page_size);

        let page_offset = if root_page == 1 {
            base_offset + 100
        } else {
            base_offset
        };

        let page = Page::read_from_bytes(bytes, &mut page_offset.clone(), &db_header, base_offset)?;

        let cells = page.leaf_table_cells();

        let data = cells
            .into_iter()
            .map(|cell| Row {
                data: std::iter::zip(columns.iter(), cell.record.0.into_iter())
                    .map(|(name, value)| (name.name.clone(), value))
                    .collect::<HashMap<_, _>>(),
                row_id: cell.row_id.0,
                primary_key_column: primary_key_column.clone(),
            })
            .collect::<Vec<_>>();

        let columns = columns
            .into_iter()
            .map(|column| (column.name.clone(), column))
            .collect::<HashMap<_, _>>();

        Ok(Self {
            name,
            table_name,
            root_page,
            sql,
            data,
            columns,
        })
    }
}

pub struct Database {
    pub header: DatabaseHeader,
    pub tables: HashMap<String, TableData>,
}

impl ReadFromBytes for Database {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let header = DatabaseHeader::read_from_bytes(&bytes, index)?;

        let schema_sqlite =
            TableData::read(TableMetadata::sqlite_schema_metadata(), &header, bytes)?;

        let mut tables = schema_sqlite
            .data
            .iter()
            .map(|row| {
                let table_data =
                    TableData::read(TableMetadata::from_sqlite_schema(row)?, &header, &bytes)?;
                Ok((table_data.table_name.clone(), table_data))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        tables.insert(schema_sqlite.table_name.clone(), schema_sqlite);

        Ok(Self { header, tables })
    }
}

impl PartialEq<SqlValue> for SerialType {
    fn eq(&self, other: &SqlValue) -> bool {
        match (self, other) {
            (SerialType::Null, _) => false,
            (SerialType::F64(_), _) => false,
            (SerialType::I0, _) => false,
            (SerialType::I1, _) => false,
            (SerialType::Blob(_), _) => false,
            (SerialType::U8(a), SqlValue::Integer(b)) => i128::from(*a) == *b,
            (SerialType::U16(a), SqlValue::Integer(b)) => i128::from(*a) == *b,
            (SerialType::U24(a), SqlValue::Integer(b)) => i128::from(*a) == *b,
            (SerialType::U32(a), SqlValue::Integer(b)) => i128::from(*a) == *b,
            (SerialType::U48(a), SqlValue::Integer(b)) => i128::from(*a) == *b,
            (SerialType::U64(a), SqlValue::Integer(b)) => i128::from(*a) == *b,
            (SerialType::Text(a), SqlValue::String(b)) => a == b,
            _ => false,
        }
    }
}

fn evaluate_condition(row: &Row, condition: &Condition) -> bool {
    match condition {
        Condition::Simple(condition) => match condition {
            SimpleCondition::Equal(column, value) => row.get(column).map_or(false, |v| &v == value),
        },
        Condition::And(left, right) => {
            evaluate_condition(row, left) && evaluate_condition(row, right)
        }
        Condition::Or(left, right) => {
            evaluate_condition(row, left) || evaluate_condition(row, right)
        }
    }
}

impl Database {
    pub fn sqlite_schema_table(&self) -> Result<&TableData> {
        self.tables
            .get("sqlite_schema")
            .ok_or_else(|| anyhow!("'sqlite_schema' table not found"))
    }

    pub fn table_names(&self) -> Result<Vec<String>> {
        Database::sqlite_schema_table(&self)?
            .data
            .iter()
            .map(|cell| {
                if let SerialType::Text(name) = cell.get("name").ok_or_else(|| anyhow!("name"))? {
                    Ok(name.clone())
                } else {
                    bail!("Expected table name to be a text")
                }
            })
            .collect()
    }

    pub fn run_query(&self, sql: SqlStatement) -> Result<Vec<Vec<SerialType>>> {
        let select_stmt = match sql {
            SqlStatement::Select(select_stmt) => select_stmt,
            _ => bail!("Only SELECT statements are currently supported"),
        };

        let table = self
            .tables
            .get(&select_stmt.table_name)
            .ok_or_else(|| anyhow!("Table not found"))?;

        #[cfg(debug_assertions)]
        eprintln!("table: {:?}", table);

        let rows = table.data.iter().filter(|row| {
            if let Some(condition) = &select_stmt.condition {
                evaluate_condition(row, condition)
            } else {
                true
            }
        });

        if let Some(Selection::Count) = select_stmt.selections.first() {
            if select_stmt.selections.len() != 1 {
                bail!("When COUNT(*) is selected, it must be the only selection");
            }
            Ok(vec![vec![SerialType::U64(
                u64::try_from(rows.count())
                    .with_context(|| "Failed to convert 'SELECT COUNT(*)'`s row count to u64")?,
            )]])
        } else {
            rows.map(|row| {
                Ok(select_stmt
                    .selections
                    .iter()
                    .map(|selection| -> Result<Vec<_>, _> {
                        Ok(match selection {
                            Selection::All => table
                                .columns
                                .keys()
                                .map(|col_name| {
                                    row.get(&col_name)
                                        .ok_or_else(|| anyhow!("Column not found"))
                                })
                                .collect::<Result<_, _>>()
                                .with_context(|| {
                                    "Unreachable: Column not found for 'SELECT *' statement"
                                })?,
                            Selection::Count => {
                                bail!("COUNT(*) must be the only selection")
                            }
                            Selection::Column(name) => {
                                vec![row.get(name).unwrap_or(SerialType::Null)]
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>())
            })
            .collect::<Result<_, _>>()
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    #[test]
    fn test_varint() -> Result<()> {
        assert_eq!(Varint::read_from_bytes(&[0x17], &mut 0)?.0, 23);
        assert_eq!(Varint::read_from_bytes(&[0x1b], &mut 0)?.0, 27);
        assert_eq!(Varint::read_from_bytes(&[0x81, 0x47], &mut 0)?.0, 199);
        Ok(())
    }
}
