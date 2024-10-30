use crate::{
    db::{
        encoding::SerialType,
        objects::{sqlite_schema_spec, IndexPageChain, PageChain, TablePageChain},
        sql::{Selection, SqlStatement, SqlValue},
        types::DatabaseHeader,
    },
    utils::utils::ReadFromBytes,
};
use anyhow::{anyhow, bail, Context, Result};
use std::{collections::HashMap, str::FromStr};

pub struct Database<'a> {
    header: DatabaseHeader,
    pub tables: HashMap<String, TablePageChain<'a>>,
    indexes: Vec<IndexPageChain<'a>>,
}
impl<'a> Database<'a> {
    pub fn page_size(&self) -> u16 {
        self.header.page_size
    }
    pub fn read_from_bytes(buf: &'a [u8], index: &mut usize) -> Result<Self> {
        let header = DatabaseHeader::read_from_bytes(&buf, index)?;
        let schema_sqlite = PageChain::read_from_bytes(
            buf,
            1,
            &header,
            SqlStatement::CreateTable(sqlite_schema_spec()),
        )?
        .try_as_table()
        .ok_or_else(|| anyhow!("Expected sqlite_schema to be a table"))?;

        let (tables, indexes) = schema_sqlite
            .read_row_range(..)
            .with_context(|| "Failed to read sqlite_schema table")?
            .iter()
            .map(|row| -> Result<_> {
                let page_number: u32 = row
                    .get("rootpage")
                    .ok_or_else(|| {
                        anyhow!("Expected 'rootpage' column to be present in sqlite_schema table")
                    })?
                    .try_into()?;
                let sql = SqlStatement::from_str(
                    &row.get("sql")
                        .unwrap()
                        .try_as_text()
                        .ok_or_else(|| anyhow!("Expected 'sql' column to be a text"))?,
                )
                .with_context(|| "Failed to parse SQL statement")?;
                let table_name = row
                    .get("tbl_name")
                    .unwrap()
                    .try_as_text()
                    .ok_or_else(|| anyhow!("Expected 'name' column to be a text"))?;
                let object = PageChain::read_from_bytes(buf, page_number, &header, sql)?;
                match object {
                    PageChain::Table(table) => Ok((Some((table_name, table)), None)),
                    PageChain::Index(index) => Ok((None, Some(index))),
                }
            })
            .collect::<Result<(Vec<_>, Vec<_>)>>()
            .with_context(|| "Failed to read tables and indexes")?;

        let tables = tables
            .into_iter()
            .flatten()
            .chain(std::iter::once((
                "sqlite_schema".to_string(),
                schema_sqlite,
            )))
            .collect();
        let indexes = indexes.into_iter().flatten().collect();

        Ok(Self {
            header,
            tables,
            indexes,
        })
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

impl<'a> Database<'a> {
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

        let filtered_columns = select_stmt
            .condition
            .as_ref()
            .map(|cond| cond.columns())
            .unwrap_or_default();

        let index = self
            .indexes
            .iter()
            .filter(|idx| idx.spec.table_name == select_stmt.table_name)
            .map(|idx| {
                let columns = idx
                    .spec
                    .columns
                    .iter()
                    .filter(|col| filtered_columns.contains(col.as_str()))
                    .count();
                (columns, idx)
            })
            .filter(|(columns, _)| *columns > 0)
            .max_by_key(|(a, _)| *a)
            .map(|(_, idx)| idx);

        let rows = if let Some(index) = index {
            eprintln!(
                "Using index: {:?} (columns: {:?})",
                index.spec.index_name, index.spec.columns
            );
            let condition = select_stmt
                .condition
                .as_ref()
                .expect("condition is missing despite having been checked for earlier");
            let row_ids = index.evaluate_condition(condition)?;

            eprintln!("matched row_ids: {:?}", row_ids);

            let results = row_ids
                .into_iter()
                .map(|row_id| {
                    let row = table
                        .find_row(row_id)
                        .ok_or_else(|| anyhow!("Failed to find row with id: {}", row_id));
                    match row {
                        Ok(Ok(row)) => Ok(row),
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(e),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            results
        } else {
            let rows = table.read_row_range(..)?;
            rows.into_iter()
                .filter(|row| {
                    if let Some(condition) = &select_stmt.condition {
                        row.evaluate_condition(condition)
                    } else {
                        true
                    }
                })
                .collect::<Vec<_>>()
        };
        if let Some(Selection::Count) = select_stmt.selections.first() {
            if select_stmt.selections.len() != 1 {
                bail!("When COUNT(*) is selected, it must be the only selection");
            }
            Ok(vec![vec![SerialType::U64(
                u64::try_from(rows.len())
                    .with_context(|| "Failed to convert 'SELECT COUNT(*)'`s row count to u64")?,
            )]])
        } else {
            rows.into_iter()
                .map(|row| {
                    Ok(select_stmt
                        .selections
                        .iter()
                        .map(|selection| -> Result<Vec<_>, _> {
                            Ok(match selection {
                                Selection::All => table
                                    .column_defs
                                    .iter()
                                    .map(|col| {
                                        row.get(&col.name)
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
