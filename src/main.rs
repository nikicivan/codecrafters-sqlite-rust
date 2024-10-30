use crate::db::db::Database;
use anyhow::{anyhow, bail, Context, Result};
use db::sql::SqlStatement;
use std::{fs::File, io::prelude::*, str::FromStr};

mod db;
mod utils;

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

            let db = Database::read_from_bytes(&contents, &mut index)?;

            println!("database page size: {}", db.page_size());

            let query_result: u64 = db
                .run_query(SqlStatement::from_str(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table'",
                )?)?
                .into_iter()
                .flatten()
                .next()
                .expect("query result not found")
                .try_into()
                .expect("query result not an integer");

            println!("number of tables: {}", query_result);
        }
        ".tables" => {
            let mut index = 0;

            let db = Database::read_from_bytes(&contents, &mut index)?;

            let query_result: Vec<_> = db
                .run_query(SqlStatement::from_str(
                    "SELECT tbl_name FROM sqlite_schema WHERE type = 'table'",
                )?)?
                .into_iter()
                .flatten()
                .map(|row| {
                    row.try_as_text()
                        .ok_or_else(|| anyhow!("Expected 'tbl_name' column to be a text"))
                })
                .collect::<Result<_, _>>()
                .expect("query result not an integer");

            println!("{}", query_result.join(" "));
        }
        query => {
            let mut index = 0;

            let db = Database::read_from_bytes(&contents, &mut index)?;

            #[cfg(debug_assertions)]
            eprintln!("Parsing query: {}", query);
            let query =
                SqlStatement::from_str(&query).with_context(|| "main: Failed to parse query")?;

            #[cfg(debug_assertions)]
            eprintln!("Executing query: {:?}", query);

            let result = db.run_query(query)?;

            for row in result {
                println!(
                    "{}",
                    row.into_iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join("|")
                );
            }
        }
    }
    Ok(())
}
