use super::*;

pub(super) struct Cursor<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) pos: usize,
}

impl Cursor<'_> {
    pub(super) fn u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub(super) fn u64(&mut self) -> Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub(super) fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?).context("WAL string is not UTF-8")
    }

    pub(super) fn option_string(&mut self) -> Result<Option<String>> {
        let present = self.take(1)?[0];
        match present {
            0 => Ok(None),
            1 => Ok(Some(self.string()?)),
            _ => crate::broker_bail!("invalid optional string marker"),
        }
    }

    pub(super) fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    pub(super) fn take(&mut self, len: usize) -> Result<&[u8]> {
        if self.pos + len > self.bytes.len() {
            crate::broker_bail!("truncated WAL record");
        }
        let start = self.pos;
        self.pos += len;
        Ok(&self.bytes[start..self.pos])
    }

    pub(super) fn finish(&self) -> Result<()> {
        crate::broker_ensure!(self.pos == self.bytes.len(), "trailing bytes in WAL record");
        Ok(())
    }
}
