use anyhow::{bail, Result};
use commands::{dbinfo, tables, DB_NAME, TABLES_NAME};
use std::env;

mod commands;

fn main() -> Result<()> {
    let (db_name, command) = extract_args()?;

    match command.as_str() {
        DB_NAME => dbinfo::cmd(&db_name)?,
        TABLES_NAME => tables::cmd(&db_name)?,
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn extract_args() -> Result<(String, String)> {
    let args: Vec<String> = env::args().collect();

    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    Ok((args[1].clone(), args[2].clone()))
}
