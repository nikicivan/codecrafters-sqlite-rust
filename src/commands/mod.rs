pub mod dbinfo;
pub mod tables;

pub const DB_HEADER_SIZE: usize = 100;
pub const DB_SCHEMA_HEADER_SIZE: usize = 8;
pub const DB_NAME: &str = ".dbinfo";
pub const TABLES_NAME: &str = ".tables";
