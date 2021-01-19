use std::io::{BufWriter, Write};

use serde::{Deserialize, Serialize};

use crate::{Error, Message, Record, Result, Schema, State};

#[derive(Debug, Serialize, Deserialize)]
pub struct Catalog {
    pub streams: Vec<Stream>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Stream {
    pub stream: String,
    pub tap_stream_id: String,
    pub schema: serde_json::Value,
    pub table_name: Option<String>,
    pub metadata: Option<Vec<Metadata>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub metadata: serde_json::Value,
    pub breadcrumb: Vec<String>,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Context {
    pub config_path: Option<String>,
    pub catalog_path: Option<String>,
    pub state_path: Option<String>,
    pub properties_path: Option<String>,
}

impl Context {
    pub fn get_option(&self, option: &'static str) -> Result<&str> {
        (match option {
            "config" => Ok(self.config_path.as_deref()),
            "catalog" => Ok(self.catalog_path.as_deref()),
            "state" => Ok(self.state_path.as_deref()),
            "properties" => Ok(self.properties_path.as_deref()),
            _ => Err(Error::InvalidOption(option)),
        })
        .and_then(|value| value.ok_or_else(|| Error::OptionNotSet(option)))
    }

    pub fn set_option<S: Into<String>>(
        &mut self,
        option: &'static str,
        value: S,
    ) -> Result<Option<String>> {
        match option {
            "config" => Ok(self.config_path.replace(value.into())),
            "catalog" => Ok(self.catalog_path.replace(value.into())),
            "state" => Ok(self.state_path.replace(value.into())),
            "properties" => Ok(self.properties_path.replace(value.into())),
            _ => Err(Error::InvalidOption(option)),
        }
    }
}

/// Create a Tap in Rust that conforms to the Singer specification.
pub trait Tap {
    /// Runs the tap in "Discovery Mode".
    ///
    /// > Discovery mode provides a way for a tap to describe the data streams
    /// > it supports. JSON schema is used to describe the structure and type of
    /// > data for each stream. The implementation of discovery mode will depend
    /// > on the tap's data source. Some taps will hard code the schema for each
    /// > stream, while others will connect to an API that provides a
    /// > [description] of the available streams. This provides a way for the
    /// > tap to return what stream are supported. [1]
    ///
    /// [1]: https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode
    ///
    /// See [Singer's Discovery Mode documentation](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode) for more information.
    fn discover(&self, context: &mut Context) -> Result<Catalog>;

    /// Run the tap in "Sync Mode". The tap should write Schema, Record, and
    /// State messages to the writer.
    ///
    /// See Singer's [Sync Mode documentation](https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md#sync-mode) for more information.
    fn sync<W: Write>(
        &mut self,
        context: &mut Context,
        writer: &mut MessageWriter<W>,
    ) -> Result<()>;
}

struct InnerWriter<W: Write>(std::sync::Arc<std::sync::Mutex<BufWriter<W>>>);

impl<W: Write> InnerWriter<W> {
    fn new(writer: W) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(BufWriter::new(
            writer,
        ))))
    }

    fn into_inner(self) -> Result<W> {
        let mutex = std::sync::Arc::try_unwrap(self.0).map_err(|_| {
            Error::OtherError("Arc has strong ref count > 1, cannot take unwrap the inner value")
        })?;
        let inner = mutex
            .into_inner()
            .map_err(|_| Error::OtherError("mutex was poisoned"))?;
        inner
            .into_inner()
            .map_err(|e| Error::IoError(std::io::Error::from(e.error().kind())))
    }
}

impl<W: Write> Clone for InnerWriter<W> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<W: Write> Write for InnerWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.0.lock().unwrap();
        guard.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut guard = self.0.lock().unwrap();
        guard.flush()
    }
}

/// Writes Messages to the writer W
pub struct MessageWriter<W: Write> {
    inner: InnerWriter<W>,
    ser: serde_json::Serializer<InnerWriter<W>>,
}

impl<W: Write> MessageWriter<W> {}

impl MessageWriter<std::io::Stdout> {
    pub fn to_stdout() -> Self {
        Self::new(std::io::stdout())
    }
}

impl MessageWriter<Vec<u8>> {
    pub fn to_buffer() -> Self {
        Self::new(vec![])
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(Vec::with_capacity(capacity))
    }
}

impl<W> MessageWriter<W>
where
    W: std::borrow::BorrowMut<Vec<u8>> + Write,
{
    pub fn with_buffer(writer: W) -> Self {
        Self::new(writer)
    }
}

impl<W: Write> MessageWriter<W> {
    pub fn new(writer: W) -> Self {
        let inner = InnerWriter::new(writer);

        let ser = serde_json::Serializer::new(inner.clone());

        Self { ser, inner }
    }

    pub fn write_message(&mut self, message: &Message) -> Result<()> {
        message.serialize(&mut self.ser)?;
        self.write_line()?;
        Ok(())
    }

    pub fn write_record(&mut self, record: Record) -> Result<()> {
        self.write_message(&Message::Record(record))?;
        Ok(())
    }

    pub fn write_state(&mut self, state: State) -> Result<()> {
        self.write_message(&Message::State(state))?;
        Ok(())
    }

    pub fn write_schema(&mut self, schema: Schema) -> Result<()> {
        self.write_message(&Message::Schema(schema))
    }

    pub fn flush(&mut self) -> Result<()> {
        self.inner.flush()?;
        Ok(())
    }

    pub fn write_line(&mut self) -> Result<()> {
        self.inner.write(b"\n")?;
        Ok(())
    }

    /// Attempts to return the inner writer.
    ///
    /// This may fail if:
    /// - The InnerWriter has more that 1 strong reference
    /// - The InnerWriter's mutex has been poisoned
    /// - The InnerWriter encounters and error when flushing
    pub fn into_inner(self) -> Result<W> {
        // drop the writer that the serializer has, decreasing the arc's strong ref
        // count to 1
        {
            let _ = self.ser.into_inner();
        }
        self.inner.into_inner()
    }
}

impl<W: Write> Write for MessageWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod test_tap {
    #[test]
    fn it_writes_line_delimited_json() {
        let mut buffer = vec![];

        let state = super::State {
            value: serde_json::Value::String(String::from("inner")),
        };
        let state = super::Message::State(state);

        {
            let mut writer = super::MessageWriter::with_buffer(&mut buffer);

            writer.write_message(&state).unwrap();
            writer.write_message(&state).unwrap();

            writer.flush().unwrap();
        }

        let mut out = serde_json::to_vec(&state).unwrap();

        let line_char = b"\n"[0];

        let mut expected = out.clone();
        expected.push(line_char);
        expected.append(&mut out);
        expected.push(line_char);

        assert_eq!(buffer, expected);
    }
}
