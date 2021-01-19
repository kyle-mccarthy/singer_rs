use std::{
    collections::HashMap,
    io::{BufReader, Read},
};

use jsonschema::Draft;
use serde_json::Value;

use crate::{Error, Message, Record, Result, Schema, State};

/// Wraps the [jsonschema::JSONSchema] and stores [serde_json::Value] for the
/// schema. The [jsonschema::JSONSchema] takes a reference to the
/// [serde_json::Value], storing the [serde_json::Value] as a raw pointer in the
/// struct ensures that the reference lives long enough.
///
/// When the struct is dropped it converts the [serde_json::Value] back into a
/// boxed value, allowing for it to be dropped without leaking memory.
///
/// # Safety
/// This struct turns the [serde_json::Value] into a raw pointer and stores it
/// internally. This raw pointer is then dereferenced in several places which is
/// an unsafe operation.
///
/// The raw pointer as well as the [jsonschema::JSONSchema] that uses the raw
/// pointer, are not exposed publicly to prevent them from being used after
/// being freed. When this struct is dropped, the schema is dropped and pointer
/// is converted back into a boxed value and dropped.
#[derive(Debug)]
pub struct JSONSchema {
    schema: jsonschema::JSONSchema<'static>,
    value: *mut Value,
}

impl JSONSchema {
    pub fn new(value: Value) -> Result<Self> {
        let value = Box::into_raw(Box::new(value));

        let schema = jsonschema::JSONSchema::compile(unsafe { &*value })
            .map_err(|_| Error::JSONSchemaCompilationError)?;

        Ok(Self { schema, value })
    }

    pub fn with_draft(value: Value, draft: Draft) -> Result<Self> {
        let value = Box::into_raw(Box::new(value));
        let schema = jsonschema::JSONSchema::options()
            .with_draft(draft)
            .compile(unsafe { &*value })
            .map_err(|e| {
                dbg!(e);
                Error::JSONSchemaCompilationError
            })?;

        Ok(Self { value, schema })
    }

    pub fn is_valid(&self, value: &Value) -> bool {
        self.schema.is_valid(value)
    }

    pub fn validate(&self, value: &Value) -> Result<()> {
        self.schema.validate(value).map_err(|mut e| {
            Error::JSONSchemaValidationError(format!(
                "{:}",
                e.next()
                    .expect("The JSON schema's error iterator should contain at least one error.")
            ))
        })
    }
}

impl Drop for JSONSchema {
    fn drop(&mut self) {
        unsafe { Box::from_raw(self.value) };
    }
}

#[derive(Debug, Default)]
pub struct Context {
    pub schemas: HashMap<String, JSONSchema>,
}

impl Context {
    pub fn has_schema(&self, schema: &Schema) -> bool {
        self.schemas.contains_key(&schema.stream)
    }

    pub fn insert_schema(&mut self, schema: &Schema) -> Result<()> {
        dbg!(schema);

        let json_schema = JSONSchema::with_draft(schema.schema.clone(), Draft::Draft4)?;

        self.schemas.insert(schema.stream.clone(), json_schema);

        Ok(())
    }

    pub fn validate_record(&self, record: &Record) -> Result<()> {
        let json_schema = self
            .schemas
            .get(&record.stream)
            .ok_or_else(|| Error::JSONSchemaNotRegistered(record.stream.clone()))?;

        if !json_schema.is_valid(&record.record) {
            json_schema.validate(&record.record)?;
        }

        Ok(())
    }
}

pub trait Target {
    fn process_record(&mut self, record: Record) -> Result<()>;

    fn process_state(&mut self, _state: State) -> Result<()> {
        Ok(())
    }

    fn process_schema(&mut self, context: &mut Context, schema: Schema) -> Result<()> {
        if !context.has_schema(&schema) {
            context.insert_schema(&schema)
        } else {
            Ok(())
        }
    }

    fn process_reader<R: Read>(&mut self, context: &mut Context, reader: R) -> Result<()> {
        use serde_json::de::{IoRead, StreamDeserializer};

        let buf_reader = BufReader::new(reader);
        let io_reader = IoRead::new(buf_reader);

        let stream = StreamDeserializer::<IoRead<BufReader<R>>, Message>::new(io_reader);

        stream
            .map(|message| {
                let message = message?;
                match message {
                    Message::Schema(schema) => self.process_schema(context, schema),
                    Message::Record(record) => {
                        context.validate_record(&record)?;
                        self.process_record(record)
                    }
                    Message::State(state) => self.process_state(state),
                }
            })
            .collect::<Result<Vec<()>>>()?;

        Ok(())
    }
}

#[cfg(test)]
mod test_target {
    use super::*;
    use crate::tap::Catalog;

    static JSON_SCHEMA: &'static str = r#"{
  "$schema": "http://json-schema.org/schema#",
  "title": "Person",
  "type": "object",
  "properties": {
    "id": {
      "type": "integer",
      "minimum": 1
    },
    "name": {
      "type": "string"
    }
  },
  "required": [
    "id",
    "name"
  ]
}"#;

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct Person {
        pub id: usize,
        pub name: String,
    }

    pub struct PeopleTap;

    impl PeopleTap {
        pub fn schema() -> crate::Schema {
            crate::Schema {
                stream: Self::stream().into(),
                schema: serde_json::from_str(&JSON_SCHEMA).unwrap(),
                key_properties: vec!["id".into()],
                bookmark_properties: None,
            }
        }

        pub fn people() -> Vec<Person> {
            vec![
                Person {
                    id: 1,
                    name: "Vincent".to_string(),
                },
                Person {
                    id: 2,
                    name: "Jules".to_string(),
                },
                Person {
                    id: 3,
                    name: "Mia".to_string(),
                },
                Person {
                    id: 4,
                    name: "Marsellus".to_string(),
                },
            ]
        }

        pub fn stream() -> &'static str {
            "people"
        }
    }

    impl crate::tap::Tap for PeopleTap {
        fn discover(&self, _context: &mut crate::tap::Context) -> Result<Catalog> {
            unimplemented!()
        }

        fn sync<W: std::io::Write>(
            &mut self,
            _context: &mut crate::tap::Context,
            writer: &mut crate::tap::MessageWriter<W>,
        ) -> Result<()> {
            writer.write_schema(Self::schema())?;

            Self::people().into_iter().for_each(|person| {
                writer
                    .write_record(Record::new(
                        Self::stream(),
                        serde_json::value::to_value(person).unwrap(),
                    ))
                    .unwrap();
            });

            Ok(())
        }
    }

    #[test]
    fn test_basic_target() {
        use super::Target;
        use crate::tap::Tap;

        #[derive(Default, Debug)]
        struct PeopleTarget {
            people: Vec<Person>,
        }

        impl Target for PeopleTarget {
            fn process_record(&mut self, record: Record) -> Result<()> {
                self.people
                    .push(serde_json::value::from_value(record.record).unwrap());
                Ok(())
            }
        }

        let mut buffer = vec![];

        {
            let mut tap = PeopleTap;
            let mut message_writer = crate::tap::MessageWriter::with_buffer(&mut buffer);
            let mut tap_ctx = crate::tap::Context::default();

            tap.sync(&mut tap_ctx, &mut message_writer).unwrap();
        }

        let mut target = PeopleTarget::default();

        {
            let mut target_ctx = super::Context::default();

            target
                .process_reader(&mut target_ctx, buffer.as_slice())
                .unwrap();
        }

        assert_eq!(target.people.len(), 4);
    }
}
