use serde::{Deserialize, Serialize};

pub mod external;
pub mod tap;
pub mod target;

// pub use tap::{Tap, TapReader};

pub type DateTime = chrono::DateTime<chrono::Utc>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Occurs when a command fails to execute. This is differs from
    /// CommandError since it occurs when the command fails to execute
    /// rather than executing but failing with an error.
    #[error("Failed to exec the command")]
    ExecError(std::io::Error),
    /// Occurs when the command exits unsuccessfully. It contains the exit code
    /// as well as the output from stderr.
    #[error("Command failed to exit successfully. Exit code ({:0?}) \n stderr: {1}")]
    CommandError(Option<i32>, String),
    #[error("IOError {0}")]
    IoError(#[from] std::io::Error),
    #[error("Failed to deserialize the value {0}")]
    DeserializationError(#[from] serde_json::Error),
    #[error("Trying to send a message in a channel where all receivers are dropped")]
    SendError(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Invalid conversion :: found ({0}) expected ({1})")]
    InvalidConversion(&'static str, &'static str),
    #[error("File could not be found: {0}")]
    FileNotFound(String),
    #[error("Option not set: {0}")]
    OptionNotSet(&'static str),
    #[error("Option is not valid: {0}")]
    InvalidOption(&'static str),

    #[error("The JSON schema for stream {0} has not been registered")]
    JSONSchemaNotRegistered(String),
    #[error("The value could not be compiled to a JSON schema")]
    JSONSchemaCompilationError,
    #[error("The value was invalid for the JSON schema. {0}")]
    JSONSchemaValidationError(String),
    #[error("An unexpected error occurred. {0}")]
    OtherError(&'static str),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct State {
    pub(crate) value: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Schema {
    pub(crate) stream: String,
    pub(crate) schema: serde_json::Value,
    pub key_properties: Vec<String>,
    pub bookmark_properties: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Record {
    pub stream: String,
    pub record: serde_json::Value,
    pub version: Option<String>,
    pub time_extracted: Option<DateTime>,
}

impl Record {
    pub fn new<S: Into<String>>(stream: S, record: serde_json::Value) -> Self {
        Self {
            version: None,
            time_extracted: None,
            stream: stream.into(),
            record,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(
    tag = "type",
    rename_all(deserialize = "UPPERCASE", serialize = "UPPERCASE")
)]
pub enum Message {
    State(State),
    Schema(Schema),
    Record(Record),
}

impl Message {
    pub fn is_state(&self) -> bool {
        match self {
            Message::State(_) => true,
            _ => false,
        }
    }

    pub fn is_schema(&self) -> bool {
        match self {
            Message::Schema(_) => true,
            _ => false,
        }
    }

    pub fn is_record(&self) -> bool {
        match self {
            Message::Record(_) => true,
            _ => false,
        }
    }

    pub fn as_state(&self) -> Option<&State> {
        match self {
            Message::State(state) => Some(state),
            _ => None,
        }
    }

    pub fn as_schema(&self) -> Option<&Schema> {
        match self {
            Message::Schema(schema) => Some(schema),
            _ => None,
        }
    }

    pub fn as_record(&self) -> Option<&Record> {
        match self {
            Message::Record(record) => Some(record),
            _ => None,
        }
    }

    pub fn ty(&self) -> &'static str {
        match self {
            Self::State { .. } => "status",
            Self::Schema { .. } => "schema",
            Self::Record { .. } => "record",
        }
    }
}

impl From<State> for Message {
    fn from(state: State) -> Self {
        Self::State(state)
    }
}

impl std::convert::TryFrom<Message> for State {
    type Error = Error;

    fn try_from(m: Message) -> Result<Self> {
        match m {
            Message::State(state) => Ok(state),
            _ => Err(Error::InvalidConversion("state", m.ty())),
        }
    }
}

impl std::convert::TryFrom<Message> for Schema {
    type Error = Error;

    fn try_from(m: Message) -> Result<Self> {
        match m {
            Message::Schema(schema) => Ok(schema),
            _ => Err(Error::InvalidConversion("schema", m.ty())),
        }
    }
}

impl std::convert::TryFrom<Message> for Record {
    type Error = Error;

    fn try_from(m: Message) -> Result<Self> {
        match m {
            Message::Record(record) => Ok(record),
            _ => Err(Error::InvalidConversion("record", m.ty())),
        }
    }
}

#[cfg(test)]
mod tests {
    // use super::{external::ExternalTap, *};

    static TAP: &'static str = "/Volumes/CODE/python/env/bin/tap-github";
    static CONFIG: &'static str = "/Volumes/CODE/python/config.json";
    static CATALOG: &'static str = "/Volumes/CODE/python/properties.json";

    #[test]
    fn it_works() {
        // let mut tap = ExternalTap::new(TAP, CONFIG, None, None);
        // tap.set_properties_path(CATALOG);

        // let reader = tap.try_into_reader().unwrap();

        // reader.for_each(|message| {
        //     dbg!(message.unwrap());
        // })
    }

    // // #[test]
    // fn test_target() {
    //     #[derive(Debug, Default)]
    //     struct ExTarget {
    //         schema: Option<Schema>,
    //         records: Vec<Record>,
    //     };

    //     impl Target for ExTarget {
    //         type Config = ();

    //         fn handle_message(&mut self, message: super::Message) {
    //             match message {
    //                 Message::Schema(schema) => {
    //                     self.schema.replace(schema);
    //                 }
    //                 Message::Record(record) => {
    //                     self.records.push(record);
    //                 }
    //                 _ => {}
    //             }
    //         }

    //         fn create(_: Self::Config) -> Self {
    //             Self::default()
    //         }
    //     }

    //     let schema = Schema {
    //         stream: "test_stream".into(),
    //         schema: serde_json::Value::Null,
    //     };

    //     let records = vec![
    //         Record {
    //             stream: "test_stream".into(),
    //             record: serde_json::Value::Null,
    //         },
    //         Record {
    //             stream: "test_stream".into(),
    //             record: serde_json::Value::Null,
    //         },
    //     ];

    //     // let (tx, rx) = channel();

    //     // tx.send(Message::Schema(schema)).unwrap();
    //     // records.into_iter().for_each(|r| {
    //     //     tx.send(Message::Record(r)).unwrap();
    //     // });

    //     // drop(tx);

    //     // let mut t = ExTarget::default();
    //     // t.process_messages(rx);

    //     // assert!(t.schema.is_some());
    //     // assert_eq!(t.records.len(), 2);
    // }
}
