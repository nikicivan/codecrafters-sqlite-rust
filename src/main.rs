use anyhow::{anyhow, bail, Context, Result};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::prelude::*;
use thiserror::Error;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }
    let database_path = &args[1];
    // Parse command and act accordingly
    let command = &args[2];

    let mut file = File::open(database_path)?;
    let mut contents = vec![];
    file.read_to_end(&mut contents)?;

    assert!(
        contents.len() <= 1073741824,
        "File is large enough to require a lock-byte page, which isn't currently implemented"
    );

    match command.as_str() {
        ".dbinfo" => {
            let mut index = 0;
            let header = DatabaseHeader::read_from_bytes(&contents, &mut index)?;
            // Uncomment this block to pass the first stage
            println!("database page size: {}", header.page_size);
            eprintln!("database header: {header:?}");
            let page_header = PageHeader::read_from_bytes(&contents, &mut index)?;
            println!("number of tables: {}", page_header.number_of_cells);
            eprintln!("page header: {page_header:?}");
        }
        ".tables" => {
            let mut index = 0;
            let header = DatabaseHeader::read_from_bytes(&contents, &mut index)?;

            let Page::LeafTable { cells, .. } =
                Page::read_from_bytes(&contents, &mut index, &header.database_text_encoding, 0)?;

            let table_data = cells
                .into_iter()
                .map(|cell| TableData::try_from(cell.record.0))
                .collect::<Result<Vec<_>>>()?;

            println!(
                "{}",
                table_data
                    .iter()
                    .map(|t| t.table_name.clone())
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }
        query => {
            let query = query.to_lowercase();
            let mut index = 0;

            let header = DatabaseHeader::read_from_bytes(&contents, &mut index)?;

            let Page::LeafTable { cells, .. } =
                Page::read_from_bytes(&contents, &mut index, &header.database_text_encoding, 0)?;

            eprintln!("cells: {cells:?}");

            let table_data = cells
                .into_iter()
                .map(|cell| {
                    let table_data = TableData::try_from(cell.record.0)?;
                    Ok((table_data.table_name.clone(), table_data))
                })
                .collect::<Result<HashMap<_, _>>>()?;

            eprintln!("table_data: {table_data:?}");

            let count_query_prefix = "select count(*) from ";

            assert!(
                query.starts_with(count_query_prefix),
                "Only COUNT queries are currently supported"
            );

            let table_name = &query[count_query_prefix.len()..];

            let table = table_data
                .get(table_name)
                .with_context(|| format!("Table {} not found", table_name))?;

            eprintln!("table: {table:?}");

            let page_offset = usize::from(table.root_page - 1) * usize::from(header.page_size);

            eprintln!(
                "root_page: {}, page_size: {}, page_offset: {page_offset}",
                table.root_page, header.page_size
            );

            let table_page = Page::read_from_bytes(
                &contents,
                &mut page_offset.clone(),
                &header.database_text_encoding,
                page_offset,
            )?;

            let Page::LeafTable { cells, .. } = table_page;
            eprintln!("cells: {cells:?}");
            println!("{}", cells.len());
        }
    }
    Ok(())
}

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
enum DatabaseEnconding {
    Utf8,
    Utf16Le,
    Utf16Be,
}

impl TryFrom<u32> for DatabaseEnconding {
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
struct DatabaseHeader {
    // skip: 16 bytes. must be MAGIC_STRING
    // size: 2 bytes
    page_size: u16,
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
    database_text_encoding: DatabaseEnconding,
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
            DatabaseEnconding::try_from(u32::read_from_bytes(bytes, index)?)
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

enum Page {
    // InteriorIndex,
    // InteriorTable,
    // LeafIndex,
    LeafTable {
        #[allow(dead_code)]
        page_header: PageHeader,
        cells: Vec<Cell>,
    },
}

impl Page {
    fn read_from_bytes(
        bytes: &[u8],
        index: &mut usize,
        text_encoding: &DatabaseEnconding,
        base_offset: usize,
    ) -> Result<Self> {
        let page_header = PageHeader::read_from_bytes(bytes, index)?;
        eprintln!("page_header: {page_header:?}");

        assert_eq!(
            page_header.page_type,
            PageType::LeafTable,
            "Only LeafTable pages are currently supported"
        );

        let cell_pointers = (0..page_header.number_of_cells)
            .map(|_| u16::read_from_bytes(bytes, index))
            .collect::<Result<Vec<_>>>()
            .with_context(|| "Failed to read cell pointers")?;

        eprintln!("cell_pointers: {cell_pointers:?}");

        let cells = cell_pointers
            .iter()
            .map(|&offset| {
                let mut index = base_offset + usize::from(offset);
                Cell::read_from_bytes(bytes, &mut index, text_encoding)
            })
            .collect::<Result<Vec<_>>>()
            .with_context(|| "Failed to read cells")?;
        Ok(Self::LeafTable { page_header, cells })
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct PageHeader {
    page_type: PageType,
    first_freeblock_offset: u16,
    number_of_cells: u16,
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

trait ReadFromBytes: Sized {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self>;
}

impl ReadFromBytes for u8 {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = bytes[*index];
        *index += 1;
        Ok(value)
    }
}

impl ReadFromBytes for u16 {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = Self::from_be_bytes(
            ReadFromBytes::read_from_bytes(bytes, index)
                .with_context(|| "Failed to convert bytes to u16")?,
        );
        Ok(value)
    }
}

impl ReadFromBytes for u32 {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = Self::from_be_bytes(
            ReadFromBytes::read_from_bytes(bytes, index)
                .with_context(|| "Failed to convert bytes to u32")?,
        );
        Ok(value)
    }
}

impl ReadFromBytes for u64 {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = Self::from_be_bytes(
            ReadFromBytes::read_from_bytes(bytes, index)
                .with_context(|| "Failed to convert bytes to u64")?,
        );
        Ok(value)
    }
}

impl ReadFromBytes for f64 {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = Self::from_be_bytes(
            ReadFromBytes::read_from_bytes(bytes, index)
                .with_context(|| "Failed to convert bytes to u64")?,
        );
        Ok(value)
    }
}

impl<const N: usize> ReadFromBytes for [u8; N] {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = bytes[*index..*index + N]
            .try_into()
            .with_context(|| format!("Failed to convert bytes to [u8; {N}]"))?;
        *index += N;
        Ok(value)
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

#[derive(Debug)]
enum SerialType {
    Null,
    #[allow(dead_code)]
    U8(u8),
    #[allow(dead_code)]
    U16(u16),
    #[allow(dead_code)]
    U24(u32),
    #[allow(dead_code)]
    U32(u32),
    #[allow(dead_code)]
    U48(u64),
    #[allow(dead_code)]
    U64(u64),
    #[allow(dead_code)]
    F64(f64),
    I0,
    I1,
    #[allow(dead_code)]
    Blob(Vec<u8>),
    Text(String),
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
        text_encoding: &DatabaseEnconding,
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
                if text_encoding != &DatabaseEnconding::Utf8 && size % 2 != 0 {
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
                    DatabaseEnconding::Utf8 => {
                        String::from_utf8(bytes).with_context(|| "Invalid UTF-8")?
                    }
                    DatabaseEnconding::Utf16Le => String::from_utf16(
                        &bytes
                            .chunks_exact(2)
                            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                            .collect::<Vec<_>>(),
                    )
                    .with_context(|| "Invalid UTF-16")?,
                    DatabaseEnconding::Utf16Be => String::from_utf16(
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
struct Record(Vec<SerialType>);

impl Record {
    fn read_from_bytes(
        bytes: &[u8],
        index: &mut usize,
        text_encoding: &DatabaseEnconding,
    ) -> Result<Self> {
        let initial_index = *index;
        let header_size: usize = Varint::read_from_bytes(bytes, index)?
            .0
            .try_into()
            .with_context(|| format!("Failed to convert record header size to usize"))?;

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
struct Cell {
    #[allow(dead_code)]
    row_id: Varint,
    record: Record,
}

impl Cell {
    fn read_from_bytes(
        bytes: &[u8],
        index: &mut usize,
        text_encoding: &DatabaseEnconding,
    ) -> Result<Self> {
        let size: usize = Varint::read_from_bytes(bytes, index)?
            .0
            .try_into()
            .with_context(|| format!("Failed to convert cell size to usize"))?;

        eprintln!("cell_size: {}", size);

        let row_id = Varint::read_from_bytes(bytes, index)?;

        eprintln!("row_id: {:?}", row_id);

        let initial_index = *index;
        let record = Record::read_from_bytes(bytes, index, text_encoding)?;

        eprintln!("record: {:?}", record);

        if *index - initial_index != size {
            bail!(
                "Expected cell payload size to be {}, got {}",
                size,
                *index - initial_index
            );
        }
        Ok(Cell { row_id, record })
    }
}

#[derive(Debug)]
struct TableData {
    #[allow(dead_code)]
    name: String,
    table_name: String,
    root_page: u8,
    #[allow(dead_code)]
    sql: String,
}

impl TryFrom<Vec<SerialType>> for TableData {
    type Error = anyhow::Error;
    fn try_from(cols: Vec<SerialType>) -> Result<Self> {
        let mut cols = VecDeque::from(cols);
        eprintln!("TableData::try_from({:?})", cols);
        match cols.pop_front() {
            Some(SerialType::Text(name)) if name == "table" => {}
            Some(SerialType::Text(name)) => bail!("Expected type to be \"table\", got {name:?}"),
            _ => bail!("Expected type to be a text"),
        };
        let name = match cols.pop_front() {
            Some(SerialType::Text(name)) => name,
            _ => bail!("Expected table name to be a text"),
        };
        let table_name = match cols.pop_front() {
            Some(SerialType::Text(table_name)) => table_name,
            _ => bail!("Expected table name to be a text"),
        };
        let root_page = match cols.pop_front() {
            Some(SerialType::U8(root_page)) => root_page,
            _ => bail!("Expected root page to be a u8"),
        };
        let sql = match cols.pop_front() {
            Some(SerialType::Text(sql)) => sql,
            _ => bail!("Expected SQL to be a text"),
        };
        Ok(Self {
            name,
            table_name,
            root_page,
            sql,
        })
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
