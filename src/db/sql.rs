use anyhow::{anyhow, bail, Result};
use itertools::*;
use std::str::FromStr;
use strum::{EnumIter, EnumString, IntoEnumIterator};

#[derive(Debug, PartialEq)]
pub enum SqlStatement {
    Select(SelectStatement),
    Create,
}

#[derive(Debug, PartialEq)]
pub enum Selection {
    All,
    Column(String),
    Count,
}

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub table_name: String,
    pub selections: Vec<Selection>,
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
}
#[derive(Debug, PartialEq)]
pub enum SqlValue {
    String(String),
    Integer(i64),
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
                .find(|keyword| s.to_ascii_uppercase().starts_with(&keyword.to_string()))
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
                        .find(|c: char| !c.is_ascii_alphabetic())
                        .unwrap_or(s.len());

                    cursor += end;
                    SqlToken::Reference(s[..end].to_string())
                }
                s if s.starts_with('\'') => {
                    let end_index = s[1..].find('\'').expect("Unterminated string") + 1;
                    SqlToken::Value(SqlValue::String(s[1..end_index].to_string()))
                }
                s if s.chars().next()?.is_ascii_digit() => {
                    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
                    let value = &s[..end];

                    match value.parse::<i64>() {
                        Ok(value) => SqlToken::Value(SqlValue::Integer(value)),
                        Err(_) => return Some(Err(anyhow!("Invalid integer: {value}"))),
                    }
                }
                _ => todo!(),
            }))
        })
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
            SqlKeyword::Create => Ok(SqlStatement::Create),
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
                Ok(SqlStatement::Select(SelectStatement {
                    table_name,
                    selections,
                }))
            }
            _ => todo!(),
        }
    }
}

impl FromStr for SqlStatement {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        SqlStatement::from_tokens(SqlToken::read(s).collect::<Result<Vec<_>>>()?.into_iter())
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
            })
        );
        assert_eq!(
            SqlStatement::from_str("SELECT COUNT(*) FROM table")?,
            SqlStatement::Select(SelectStatement {
                table_name: "table".to_string(),
                selections: vec![Selection::Count],
            })
        );
        assert_eq!(
            SqlStatement::from_str("SELECT a, b FROM table")?,
            SqlStatement::Select(SelectStatement {
                table_name: "table".to_string(),
                selections: vec![
                    Selection::Column("a".to_string()),
                    Selection::Column("b".to_string())
                ],
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
            })
        );
        Ok(())
    }
}
