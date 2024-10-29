use anyhow::Result;
use std::fs::File;
use std::os::unix::fs::FileExt;

use super::{dbinfo, DB_HEADER_SIZE, DB_SCHEMA_HEADER_SIZE};

pub fn cmd(filename: &str) -> Result<()> {
    let mut db = File::open(filename)?;

    let offsets = get_table_offsets(&mut db)?;

    let table_names = offsets
        .iter()
        .map(|offset| get_table_name(&mut db, *offset))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .filter(|name| name != "sqlite_sequence")
        .collect::<Vec<_>>()
        .join(" ");

    println!("{table_names}");

    Ok(())
}

pub fn get_table_offsets(db: &mut File) -> Result<Vec<u16>> {
    let cell_pointer_array_offset = DB_HEADER_SIZE + DB_SCHEMA_HEADER_SIZE;
    let num_tables = dbinfo::num_tables(db)?;

    let mut cell_pointer_array = vec![0u8; 2 * num_tables as usize];
    db.read_exact_at(&mut cell_pointer_array, cell_pointer_array_offset as u64)?;

    let table_offsets = cell_pointer_array
        .chunks(2)
        .map(|be| u16::from_be_bytes([be[0], be[1]]))
        .collect();

    Ok(table_offsets)
}

pub fn get_table_name(db: &mut File, record_offset: u16) -> Result<String> {
    let record_meta_size = 2;

    let mut record_meta_and_header_size = vec![0; record_meta_size + 1];
    db.read_exact_at(&mut record_meta_and_header_size, record_offset as u64)?;

    let header_size = record_meta_and_header_size[2] as usize;
    let header_offset = record_offset as usize + record_meta_size;
    let mut header = vec![0; header_size];
    db.read_exact_at(&mut header, header_offset as u64)?;

    let sizes: Vec<usize> = header[1..4].iter().map(varint_type_size).collect();
    let [type_size, name_size, table_name_size] = [sizes[0], sizes[1], sizes[2]];
    let table_name_offset = header_offset + header_size + type_size + name_size;

    let mut table_name = vec![0; table_name_size];
    db.read_exact_at(&mut table_name, table_name_offset as u64)?;

    Ok(String::from_utf8(table_name)?.trim_end().to_string())
}

fn varint_type_size(varint_type: &u8) -> usize {
    let vtype = *varint_type as usize;
    match vtype {
        0 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 | 7 => 8,
        8 | 9 => 0,
        10 | 11 => 0,
        _ => {
            let base = if vtype % 2 == 0 { 12 } else { 13 };
            (vtype - (base)) / 2
        }
    }
}
