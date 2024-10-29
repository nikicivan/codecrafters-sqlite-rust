use anyhow::Result;
use std::fs::File;
use std::io::prelude::*;
use std::os::unix::fs::FileExt;

use super::{DB_HEADER_SIZE, DB_SCHEMA_HEADER_SIZE};

pub fn cmd(filename: &str) -> Result<()> {
    let mut db = File::open(filename)?;

    println!("database page size: {}", database_page_size(&mut db)?);
    println!("number of tables: {}", num_tables(&mut db)?);

    Ok(())
}

pub fn database_page_size(db: &mut File) -> Result<u16> {
    let mut header = [0; DB_HEADER_SIZE];
    db.read_exact(&mut header)?;

    Ok(u16::from_be_bytes([header[16], header[17]]))
}

pub fn num_tables(db: &mut File) -> Result<u16> {
    let mut header = [0; DB_SCHEMA_HEADER_SIZE];
    let offset = DB_HEADER_SIZE as u64; // schema header is after the db header

    db.read_exact_at(&mut header, offset)?;

    Ok(u16::from_be_bytes([header[3], header[4]]))
}
