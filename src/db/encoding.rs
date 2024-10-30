use crate::{db::types::DatabaseEncoding, utils::utils::ReadFromBytes};
use anyhow::{anyhow, bail, Context, Result};
use std::cmp::Ordering;
use strum::{EnumIs, EnumTryAs};

#[derive(Debug)]
pub struct Varint(pub u64);
impl ReadFromBytes for Varint {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        type Num = u64;
        let mut value: Num = 0;
        for i in 0..9 {
            let byte = u8::read_from_bytes(bytes, index)
                .with_context(|| format!("Failed to read byte #{} of Varint ({value:#b})", i))?;
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

#[derive(Debug, Clone, EnumIs, EnumTryAs, PartialEq)]
pub enum SerialType {
    Null,
    U8(u8),
    U16(u16),
    U24(u32),
    U32(u32),
    U48(u64),
    U64(u64),
    F64(f64),
    I0,
    I1,
    Blob(Vec<u8>),
    Text(String),
}

impl TryInto<u64> for SerialType {
    type Error = anyhow::Error;
    fn try_into(self) -> std::result::Result<u64, Self::Error> {
        match self {
            SerialType::I0 => Ok(0),
            SerialType::I1 => Ok(1),
            // FIXME: are these /\ two correct?
            SerialType::U8(value) => Ok(u64::from(value)),
            SerialType::U16(value) => Ok(u64::from(value)),
            SerialType::U24(value) => Ok(u64::from(value)),
            SerialType::U32(value) => Ok(u64::from(value)),
            SerialType::U48(value) => Ok(value),
            SerialType::U64(value) => Ok(value),
            _ => Err(anyhow!("Failed to convert {:?} to u64", self)),
        }
    }
}

impl TryInto<u32> for SerialType {
    type Error = anyhow::Error;
    fn try_into(self) -> std::result::Result<u32, Self::Error> {
        match self {
            SerialType::I0 => Ok(0),
            SerialType::I1 => Ok(1),
            // FIXME: are these /\ two correct?
            SerialType::U8(value) => Ok(u32::from(value)),
            SerialType::U16(value) => Ok(u32::from(value)),
            SerialType::U24(value) => Ok(u32::from(value)),
            SerialType::U32(value) => Ok(u32::from(value)),
            _ => Err(anyhow!("Failed to convert {:?} to u64", self)),
        }
    }
}

impl PartialOrd for SerialType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (SerialType::Null, SerialType::Null) => Some(Ordering::Equal),
            (SerialType::U8(a), SerialType::U8(b)) => a.partial_cmp(b),
            (SerialType::U16(a), SerialType::U8(b)) => a.partial_cmp(&u16::from(*b)),
            (SerialType::U16(a), SerialType::U16(b)) => a.partial_cmp(b),
            (SerialType::U24(a), SerialType::U8(b)) => a.partial_cmp(&u32::from(*b)),
            (SerialType::U24(a), SerialType::U16(b)) => a.partial_cmp(&u32::from(*b)),
            (SerialType::U24(a), SerialType::U24(b)) => a.partial_cmp(b),
            (SerialType::U32(a), SerialType::U8(b)) => a.partial_cmp(&u32::from(*b)),
            (SerialType::U32(a), SerialType::U16(b)) => a.partial_cmp(&u32::from(*b)),
            (SerialType::U32(a), SerialType::U24(b)) => a.partial_cmp(b),
            (SerialType::U32(a), SerialType::U32(b)) => a.partial_cmp(b),
            (SerialType::U48(a), SerialType::U8(b)) => a.partial_cmp(&u64::from(*b)),
            (SerialType::U48(a), SerialType::U16(b)) => a.partial_cmp(&u64::from(*b)),
            (SerialType::U48(a), SerialType::U24(b)) => a.partial_cmp(&u64::from(*b)),
            (SerialType::U48(a), SerialType::U32(b)) => a.partial_cmp(&u64::from(*b)),
            (SerialType::U48(a), SerialType::U48(b)) => a.partial_cmp(b),
            (SerialType::U64(a), SerialType::U8(b)) => a.partial_cmp(&u64::from(*b)),
            (SerialType::U64(a), SerialType::U16(b)) => a.partial_cmp(&u64::from(*b)),
            (SerialType::U64(a), SerialType::U24(b)) => a.partial_cmp(&u64::from(*b)),
            (SerialType::U64(a), SerialType::U32(b)) => a.partial_cmp(&u64::from(*b)),
            (SerialType::U64(a), SerialType::U48(b)) => a.partial_cmp(b),
            (SerialType::U64(a), SerialType::U64(b)) => a.partial_cmp(b),
            (SerialType::F64(a), SerialType::F64(b)) => a.partial_cmp(b),
            (SerialType::Text(a), SerialType::Text(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

// impl PartialEq for SerialType {
//     fn eq(&self, other: &Self) -> bool {
//         self.partial_cmp(other)
//             .map_or(false, |ord| ord == Ordering::Equal)
//     }
// }

impl std::fmt::Display for SerialType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerialType::I0 => write!(f, "I0"),
            SerialType::I1 => write!(f, "I1"),
            SerialType::Null => write!(f, "NULL"),
            SerialType::U8(value) => write!(f, "{value}"),
            SerialType::U16(value) => write!(f, "{value}"),
            SerialType::U24(value) => write!(f, "{value}"),
            SerialType::U32(value) => write!(f, "{value}"),
            SerialType::U48(value) => write!(f, "{value}"),
            SerialType::U64(value) => write!(f, "{value}"),
            SerialType::F64(value) => write!(f, "{value}"),
            SerialType::Text(value) => write!(f, "{value}"),
            SerialType::Blob(value) => write!(f, "{value:?}"),
        }
    }
}

#[derive(Debug)]
pub enum SerialTypeDescription {
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
    pub fn read_from_bytes(
        &self,
        bytes: &[u8],
        index: &mut usize,
        text_encoding: &DatabaseEncoding,
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
                if text_encoding != &DatabaseEncoding::Utf8 && size % 2 != 0 {
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
                    DatabaseEncoding::Utf8 => {
                        String::from_utf8(bytes).with_context(|| "Invalid UTF-8")?
                    }
                    DatabaseEncoding::Utf16Le => String::from_utf16(
                        &bytes
                            .chunks_exact(2)
                            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                            .collect::<Vec<_>>(),
                    )
                    .with_context(|| "Invalid UTF-16")?,
                    DatabaseEncoding::Utf16Be => String::from_utf16(
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
