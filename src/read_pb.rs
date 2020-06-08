use indicatif::ProgressBar;
use std::io::{self, prelude::*};

/// Wrapper around std::io::Read that updates a progress bar with number of bytes read
pub struct ReadProgressBar<R> {
    reader: R,
    progress_bar: ProgressBar,
}

impl<R> ReadProgressBar<R>
where
    R: Read,
{
    pub fn new(reader: R, progress_bar: ProgressBar) -> Self {
        Self {
            reader,
            progress_bar,
        }
    }
}

impl<R> Read for ReadProgressBar<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let bytes_read = self.reader.read(buf)?;

        self.progress_bar.inc(bytes_read as u64);

        Ok(bytes_read)
    }
}
