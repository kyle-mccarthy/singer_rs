use std::{
    io::{BufRead, BufReader},
    process::{Command, Stdio},
};

use serde_json::Value;

use crate::{
    tap::{Catalog, MessageWriter, Tap},
    Error, Result,
};

/// Allows for interacting with a tap that isn't implemented in rust. Running an
/// external tap executes the program in a child process and processes messages
/// written to stdout.
pub struct ExternalTap {
    /// The tap to execute. The program for the tap is resolved exactly as
    /// [`std::process::Command::new`] resolves programs:
    ///
    /// > If program is not an absolute path, the PATH will be searched in an
    /// OS-defined way.
    ///
    /// See [command's docs] for additional information.
    ///
    /// [command's docs]: std::process::Command#method.new
    pub tap: String,
}

impl ExternalTap {
    pub fn new<S: Into<String>>(tap: S) -> Self {
        Self { tap: tap.into() }
    }
}

impl Tap for ExternalTap {
    // fn options(&self) -> &TapOptions {
    //     &self.options
    // }

    // fn options_mut(&mut self) -> &mut TapOptions {
    //     &mut self.options
    // }

    /// Calls the external tap with the discover option and deserializes it into
    /// a serde_json::Value.
    fn discover(&self, context: &mut crate::tap::Context) -> Result<Catalog> {
        let config = context.get_option("config")?;

        let child = Command::new(&self.tap)
            .args(&["--config", config, "--discover"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| Error::ExecError(err))?;

        let output = child.wait_with_output()?;

        if !output.status.success() {
            let stderr: Value = serde_json::from_slice(&output.stderr)?;
            return Err(Error::CommandError(
                output.status.code(),
                serde_json::to_string(&stderr)?,
            ));
        }

        let catalog = serde_json::from_slice(&output.stdout)?;

        Ok(catalog)
    }

    /// Reads the data emitted to stdout by the tap and copies that data to the
    /// message writer.
    fn sync<W: std::io::Write>(
        &mut self,
        context: &mut crate::tap::Context,
        writer: &mut MessageWriter<W>,
    ) -> Result<()> {
        let config = context.get_option("config")?;

        let mut args = vec!["--config", config];

        if let Ok(catalog) = context.get_option("catalog") {
            args.extend(&["--catalog", catalog]);
        }

        if let Ok(state) = &context.get_option("state") {
            args.extend(&["--state", state]);
        }

        if let Ok(properties) = &context.get_option("properties") {
            args.extend(&["--properties", properties]);
        }

        let mut child = Command::new(&self.tap)
            .args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|err| Error::ExecError(err))?;

        let mut stdout = child.stdout.take().expect(
            "piped stdout should be
Some",
        );
        let stderr = child.stderr.take().expect(
            "piped stderr should
be Some",
        );

        std::io::copy(&mut stdout, writer)?;

        let output = child.wait_with_output()?;

        if !output.status.success() {
            let errors = BufReader::new(stderr);
            let last_error = errors
                .lines()
                .last()
                .or_else(|| {
                    Some(Ok(String::from(
                        "The taps process exited with an error but didn't write any data to stderr",
                    )))
                })
                .expect(
                    "or_else should set alternative message if the last line didn't contain exist \
                     in stderr",
                )?;

            return Err(Error::CommandError(output.status.code(), last_error));
        }

        Ok(())
    }
}

#[cfg(test)]
mod test_external {
    use super::*;

    static TAP: &'static str = "/Volumes/CODE/python/env/bin/tap-github";
    static CONFIG: &'static str = "/Volumes/CODE/python/config.json";
    static CATALOG: &'static str = "/Volumes/CODE/python/properties.json";

    #[test]
    fn it_works() {
        let mut tap = ExternalTap::new(TAP);
        let mut context = crate::tap::Context::default();
        context.set_option("config", CONFIG).unwrap();
        context.set_option("properties", CATALOG).unwrap();

        let mut writer = MessageWriter::to_stdout();

        tap.sync(&mut context, &mut writer).unwrap();
    }
}
