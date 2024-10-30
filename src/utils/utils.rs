use anyhow::{Context, Result};

pub trait ReadFromBytes: Sized {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self>;
}

impl ReadFromBytes for u8 {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = bytes[*index];
        *index += 1;
        Ok(value)
    }
}

impl ReadFromBytes for u16 {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = Self::from_be_bytes(
            ReadFromBytes::read_from_bytes(bytes, index)
                .with_context(|| "Failed to convert bytes to u16")?,
        );
        Ok(value)
    }
}

impl ReadFromBytes for u32 {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = Self::from_be_bytes(
            ReadFromBytes::read_from_bytes(bytes, index)
                .with_context(|| "Failed to convert bytes to u32")?,
        );
        Ok(value)
    }
}

impl ReadFromBytes for u64 {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = Self::from_be_bytes(
            ReadFromBytes::read_from_bytes(bytes, index)
                .with_context(|| "Failed to convert bytes to u64")?,
        );
        Ok(value)
    }
}

impl ReadFromBytes for f64 {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = Self::from_be_bytes(
            ReadFromBytes::read_from_bytes(bytes, index)
                .with_context(|| "Failed to convert bytes to u64")?,
        );
        Ok(value)
    }
}

impl<const N: usize> ReadFromBytes for [u8; N] {
    fn read_from_bytes(bytes: &[u8], index: &mut usize) -> Result<Self> {
        let value = bytes[*index..*index + N]
            .try_into()
            .with_context(|| format!("Failed to convert bytes to [u8; {N}]"))?;
        *index += N;
        Ok(value)
    }
}
