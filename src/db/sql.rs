use anyhow::{anyhow, bail, Context, Result};
use itertools::*;
use std::iter::Peekable;
use std::str::FromStr;
use strum::{EnumIter, EnumString, IntoEnumIterator};

#[derive(Debug, PartialEq)]
pub enum SqlStatement {
    Select(SelectStatement),
    #[allow(dead_code)]
    Create,
    CreateTable(CreateTableStatement),
}

#[derive(Debug, PartialEq)]
pub enum Selection {
    All,
    Column(String),
    Count,
}

#[derive(Debug, PartialEq)]
pub enum SimpleCondition {
    Equal(String, SqlValue),
}

#[derive(Debug, PartialEq)]
pub enum Condition {
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Simple(SimpleCondition),
}

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub table_name: String,
    pub selections: Vec<Selection>,
    pub condition: Option<Condition>,
}

#[derive(Debug, PartialEq)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, PartialEq)]
enum SqlToken {
    Keyword(SqlKeyword),
    Reference(String),
    Value(SqlValue),
    FixedWidth(SqlFixedWidthToken),
}

#[derive(Debug, EnumString, strum::Display, EnumIter, PartialEq)]
enum SqlFixedWidthToken {
    #[strum(serialize = ",")]
    Comma,
    #[strum(serialize = "(")]
    LeftParen,
    #[strum(serialize = ")")]
    RightParen,
    #[strum(serialize = "*")]
    Star,
    #[strum(serialize = "=")]
    Equal,
}
#[derive(Debug, PartialEq)]
pub enum SqlValue {
    String(String),
    Integer(i128),
}

#[derive(Debug, EnumString, strum::Display, EnumIter, PartialEq)]
enum SqlKeyword {
    #[strum(serialize = "SELECT")]
    Select,
    #[strum(serialize = "FROM")]
    From,
    #[strum(serialize = "WHERE")]
    Where,
    #[strum(serialize = "COUNT")]
    Count,
    #[strum(serialize = "CREATE")]
    Create,
    #[strum(serialize = "AND")]
    And,
    #[strum(serialize = "OR")]
    Or,
    #[strum(serialize = "TABLE")]
    Table,
}

fn is_valid_identifier_char(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

impl SqlToken {
    fn read(s: &str) -> impl Iterator<Item = Result<SqlToken>> + '_ {
        let mut cursor = 0;
        std::iter::from_fn(move || {
            let s = {
                while s[cursor..].starts_with(char::is_whitespace) {
                    cursor += 1;
                }

                &s[cursor..]
            };

            let fixed_with_token_match = SqlFixedWidthToken::iter()
                .find(|token| s.starts_with(&token.to_string()))
                .map(|token| {
                    cursor += token.to_string().len();
                    Ok(SqlToken::FixedWidth(token))
                });

            if fixed_with_token_match.is_some() {
                return fixed_with_token_match;
            }

            let keyword_match = SqlKeyword::iter()
                .find(|keyword| {
                    s.to_ascii_uppercase().starts_with(&keyword.to_string())
                        && !s[keyword.to_string().len()..].starts_with(is_valid_identifier_char)
                })
                .map(|keyword| {
                    cursor += keyword.to_string().len();
                    Ok(SqlToken::Keyword(keyword))
                });

            if keyword_match.is_some() {
                return keyword_match;
            }

            Some(Ok(match s {
                s if s.is_empty() => return None,
                s if s.chars().next()?.is_ascii_alphabetic() => {
                    let end = s
                        .find(|c: char| !is_valid_identifier_char(c))
                        .unwrap_or(s.len());

                    cursor += end;
                    SqlToken::Reference(s[..end].to_string())
                }
                s if s.starts_with('\'') => {
                    let end_index = s[1..].find('\'').expect("Unterminated string") + 1;
                    cursor += end_index + 1;
                    SqlToken::Value(SqlValue::String(s[1..end_index].to_string()))
                }
                s if s.chars().next()?.is_ascii_digit() => {
                    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
                    let value = &s[..end];

                    cursor += end;

                    match value.parse() {
                        Ok(value) => SqlToken::Value(SqlValue::Integer(value)),
                        Err(_) => return Some(Err(anyhow!("Invalid integer: {value}"))),
                    }
                }
                s => return Some(Err(anyhow!("SqlToken::read: Unexpected string: {s:?}"))),
            }))
        })
    }
}

fn next_simple_condition<T: Iterator<Item = SqlToken>>(
    iter: &mut Peekable<T>,
) -> Result<SimpleCondition> {
    let reference = match iter.next() {
        Some(SqlToken::Reference(reference)) => reference,
        Some(token) => bail!("Expected reference after WHERE, got {token:?}"),
        None => bail!("Expected reference after WHERE, got EOF"),
    };

    if iter
        .next_if_eq(&SqlToken::FixedWidth(SqlFixedWidthToken::Equal))
        .is_none()
    {
        bail!("Expected '=' after WHERE reference");
    }

    let value = match iter.next() {
        Some(SqlToken::Value(value)) => value,
        Some(token) => bail!("Expected value after WHERE '=', got {token:?}"),
        None => bail!("Expected value after WHERE '=', got EOF"),
    };

    Ok(SimpleCondition::Equal(reference, value))
}

fn read_condition<T: Iterator<Item = SqlToken>>(iter: &mut Peekable<T>) -> Result<Condition> {
    let left = next_simple_condition(iter)?;

    #[cfg(debug_assertions)]
    eprintln!("read_condition: left: {:?}", left);

    if iter
        .next_if_eq(&SqlToken::Keyword(SqlKeyword::And))
        .is_some()
    {
        #[cfg(debug_assertions)]
        eprintln!("read_condition: AND");

        let right = read_condition(iter)?;

        #[cfg(debug_assertions)]
        eprintln!("read_condition: condition: {:?}", right);

        return Ok(Condition::And(
            Box::new(Condition::Simple(left)),
            Box::new(right),
        ));
    } else if iter
        .next_if_eq(&SqlToken::Keyword(SqlKeyword::Or))
        .is_some()
    {
        #[cfg(debug_assertions)]
        eprintln!("read_condition: OR");

        let right = read_condition(iter)?;

        #[cfg(debug_assertions)]
        eprintln!("read_condition: condition: {:?}", right);

        return Ok(Condition::Or(
            Box::new(Condition::Simple(left)),
            Box::new(right),
        ));
    } else {
        Ok(Condition::Simple(left))
    }
}

impl SqlStatement {
    fn from_tokens<T: Iterator<Item = SqlToken>>(iter: T) -> Result<Self> {
        let mut iter = iter.peekable();

        let keyword = match iter.next() {
            Some(SqlToken::Keyword(keyword)) => keyword,
            Some(token) => return Err(anyhow!("Expected keyword. got {token:?}")),
            None => bail!("Expected keyword, got EOF"),
        };

        match keyword {
            SqlKeyword::Create => {
                iter.next_if_eq(&SqlToken::Keyword(SqlKeyword::Table))
                    .ok_or_else(|| anyhow!("Expected TABLE after CREATE"))?;

                let table_name = match iter.next() {
                    Some(SqlToken::Reference(table_name)) => table_name,
                    Some(token) => bail!("Expected table name, got {token:?}"),
                    None => bail!("Expected table name, got EOF"),
                };

                iter.next_if_eq(&SqlToken::FixedWidth(SqlFixedWidthToken::LeftParen))
                    .ok_or_else(|| anyhow!("Expected '(' after CREATE TABLE"))?;

                let columns = std::iter::from_fn({
                    let mut iter = iter.by_ref().peeking_take_while(|token| match token {
                        SqlToken::FixedWidth(SqlFixedWidthToken::RightParen) => false,
                        _ => true,
                    });
                    move || {
                        let column = match iter.next()? {
                            SqlToken::Reference(column) => column.clone(),
                            token => return Some(Err(anyhow!("Expected column, got {token:?}"))),
                        };
                        iter.by_ref()
                            .take_while(|token| match token {
                                SqlToken::FixedWidth(SqlFixedWidthToken::Comma) => false,
                                _ => true,
                            })
                            .count();
                        Some(Ok(column))
                    }
                })
                .collect::<Result<_, _>>()?;

                iter.next_if_eq(&SqlToken::FixedWidth(SqlFixedWidthToken::RightParen))
                    .ok_or_else(|| anyhow!("Expected ')' after CREATE TABLE columns"))?;

                Ok(SqlStatement::CreateTable(CreateTableStatement {
                    table_name,
                    columns,
                }))
            }
            SqlKeyword::Select => {
                let mut selection_tokens = iter
                    .by_ref()
                    .peeking_take_while(|token| match token {
                        SqlToken::Keyword(SqlKeyword::From) => false,
                        _ => true,
                    })
                    .peekable();

                let selections = std::iter::from_fn(|| {
                    let selection = match selection_tokens.next()? {
                        SqlToken::Reference(column) => Selection::Column(column),
                        SqlToken::FixedWidth(SqlFixedWidthToken::Star) => Selection::All,
                        SqlToken::Keyword(SqlKeyword::Count) => {
                            let assertions = (|| -> Result<()> {
                                selection_tokens
                                    .next_if_eq(&SqlToken::FixedWidth(
                                        SqlFixedWidthToken::LeftParen,
                                    ))
                                    .ok_or_else(|| anyhow!("Expected '(*)' after COUNT"))?;
                                selection_tokens
                                    .next_if_eq(&SqlToken::FixedWidth(SqlFixedWidthToken::Star))
                                    .ok_or_else(|| anyhow!("Expected '(*)' after COUNT"))?;
                                selection_tokens
                                    .next_if_eq(&SqlToken::FixedWidth(
                                        SqlFixedWidthToken::RightParen,
                                    ))
                                    .ok_or_else(|| anyhow!("Expected '(*)' after COUNT"))?;
                                Ok(())
                            })();
                            match assertions {
                                Ok(()) => Selection::Count,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        token => return Some(Err(anyhow!("Expected selection, got {token:?}"))),
                    };

                    if selection_tokens.peek().is_some() {
                        let assertion = selection_tokens
                            .next_if_eq(&SqlToken::FixedWidth(SqlFixedWidthToken::Comma))
                            .ok_or_else(|| anyhow!("Expected ',' after selection"));
                        if let Err(e) = assertion {
                            return Some(Err(e));
                        }
                    }
                    Some(Ok(selection))
                })
                .collect::<Result<Vec<_>, _>>()?;

                iter.next_if_eq(&SqlToken::Keyword(SqlKeyword::From))
                    .ok_or_else(|| anyhow!("Expected FROM after SELECT"))?;

                let table_name = match iter.next() {
                    Some(SqlToken::Reference(table_name)) => table_name,
                    Some(token) => bail!("Expected table name, got {token:?}"),
                    None => bail!("Expected table name, got EOF"),
                };

                let condition = if iter
                    .next_if_eq(&SqlToken::Keyword(SqlKeyword::Where))
                    .is_some()
                {
                    #[cfg(debug_assertions)]
                    eprintln!("Found WHERE, calling read_condition");
                    Some(read_condition(&mut iter)?)
                } else {
                    None
                };

                Ok(SqlStatement::Select(SelectStatement {
                    table_name,
                    selections,
                    condition,
                }))
            }
            _ => todo!(),
        }
    }
}

impl FromStr for SqlStatement {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        let tokens = SqlToken::read(s)
            .collect::<Result<Vec<_>>>()
            .with_context(|| format!("Failed to parse SQL statement from {s:?}"))?;

        #[cfg(debug_assertions)]
        println!("tokens: {:?}", tokens);

        SqlStatement::from_tokens(tokens.into_iter())
            .with_context(|| format!("Failed to parse SQL statement from string {s:?}"))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_select_statement() -> Result<()> {
        assert_eq!(
            SqlStatement::from_str("SELECT * FROM table")?,
            SqlStatement::Select(SelectStatement {
                table_name: "table".to_string(),
                selections: vec![Selection::All],
                condition: None,
            })
        );
        assert_eq!(
            SqlStatement::from_str("SELECT COUNT(*) FROM table")?,
            SqlStatement::Select(SelectStatement {
                table_name: "table".to_string(),
                selections: vec![Selection::Count],
                condition: None,
            })
        );
        assert_eq!(
            SqlStatement::from_str("SELECT a, b FROM table")?,
            SqlStatement::Select(SelectStatement {
                table_name: "table".to_string(),
                selections: vec![
                    Selection::Column("a".to_string()),
                    Selection::Column("b".to_string()),
                ],
                condition: None,
            })
        );
        assert_eq!(
            SqlStatement::from_str("select a, b from table")?,
            SqlStatement::Select(SelectStatement {
                table_name: "table".to_string(),
                selections: vec![
                    Selection::Column("a".to_string()),
                    Selection::Column("b".to_string())
                ],
                condition: None
            })
        );
        assert_eq!(
            SqlStatement::from_str("select a, b from table where a = '1'")?,
            SqlStatement::Select(SelectStatement {
                table_name: "table".to_string(),
                selections: vec![
                    Selection::Column("a".to_string()),
                    Selection::Column("b".to_string())
                ],
                condition: Some(Condition::Simple(SimpleCondition::Equal(
                    "a".to_string(),
                    SqlValue::String("1".to_string())
                )))
            })
        );

        Ok(())
    }
}
