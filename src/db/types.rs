use anyhow::Result;
use thiserror::Error;

use crate::utils::utils::ReadFromBytes;

const MAGIC_STRING_LEN: usize = 16;
const MAGIC_STRING: [u8; MAGIC_STRING_LEN] = *b"SQLite format 3\0";

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Clone)]
pub struct DatabaseHeader {
    // skip: 16 bytes. must be MAGIC_STRING
    // size: 2 bytes
    pub page_size: u16,
    // size: 1 byte
    #[allow(dead_code)]
    file_format_write_version: FileFormatVersion,
    // size: 1 byte
    #[allow(dead_code)]
    file_format_read_version: FileFormatVersion,
    // size: 1 byte
    #[allow(dead_code)]
    reserved_space_per_page: u8,
    // size: 1 byte, must be 64
    #[allow(dead_code)]
    max_embedded_payload_fraction: u8,
    // size: 1 byte, must be 32
    #[allow(dead_code)]
    min_embedded_payload_fraction: u8,
    // size: 1 byte
    #[allow(dead_code)]
    leaf_payload_fraction: u8,
    // size: 4 bytes
    #[allow(dead_code)]
    file_change_counter: u32,
    // size: 4 bytes
    #[allow(dead_code)]
    database_size_in_pages: u32,
    // size: 4 bytes
    #[allow(dead_code)]
    first_freelist_trunk_page: u32,
    // size: 4 bytes
    #[allow(dead_code)]
    total_freelist_pages: u32,
    // size: 4 bytes
    #[allow(dead_code)]
    schema_cookie: u32,
    // size: 4 bytes. must be 1, 2, 3, or 4
    #[allow(dead_code)]
    schema_format_number: u32,
    // size: 4 bytes
    #[allow(dead_code)]
    default_page_cache_size: u32,
    // size: 4 bytes. should be 0 unless in auto- or incremental-vacuum modes
    #[allow(dead_code)]
    largest_btree_page_number: u32,
    // size: 4 bytes
    pub database_text_encoding: DatabaseEncoding,
    // size: 4 bytes
    #[allow(dead_code)]
    user_version: u32,
    // size: 4 bytes
    #[allow(dead_code)]
    is_incremental_vacuum: bool,
    // size: 4 bytes
    #[allow(dead_code)]
    application_id: u32,
    // skip: 20 bytes. must be 0
    // size: 4 bytes
    #[allow(dead_code)]
    version_valid_for: u32,
    // size: 4 bytes
    #[allow(dead_code)]
    sqlite_version_number: u32,
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
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
