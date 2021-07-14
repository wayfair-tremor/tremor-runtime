// Copyright 2020-2021, The Tremor Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub use crate::interpreter::AggrType;
use crate::{
    ast::{Docs, Helper, Warning, Warnings},
    ctx::EventContext,
    errors::{CompilerError, Error, Result},
    highlighter::{Dumb as DumbHighlighter, Highlighter},
    lexer,
    parser::g as grammar,
    path::ModulePath,
    pos::Range,
    registry::{Aggr as AggrRegistry, Registry},
    srs, Value,
};
use serde::Serialize;
use std::io::{self, Write};

/// Return of a script execution
#[derive(Debug, Serialize, PartialEq)]
pub enum Return<'event> {
    /// This script should emit the returned
    /// value
    Emit {
        /// Value to emit
        value: Value<'event>,
        /// Port to emit to
        port: Option<String>,
    },
    /// This event should be dropped
    Drop,
    /// This script should emit the event that
    /// was passed in
    EmitEvent {
        /// Port to emit to
        port: Option<String>,
    },
}

/// A tremor script
#[derive(Debug)]
pub struct Script {
    // TODO: This should probably be pulled out to allow people wrapping it themselves
    /// Rental for the runnable script
    pub script: srs::Script,
    /// Source code for this script
    pub source: String,
    /// A set of warnings if any
    pub(crate) warnings: Warnings,
}

impl Script {
    /// Get script warnings
    pub fn warnings(&self) -> impl Iterator<Item = &Warning> {
        self.warnings.iter()
    }
    /// Parses a string and turns it into a script
    ///
    /// # Errors
    /// if the script can not be parsed
    pub fn parse(
        module_path: &ModulePath,
        file_name: &str,
        script: String,
        reg: &Registry,
        // args: Option<Vec<&str>>,
        // aggr_reg: &AggrRegistry, - we really should shadow and provide a nice hygienic error TODO but not today
    ) -> std::result::Result<Self, CompilerError> {
        let mut include_stack = lexer::IncludeStack::default();
        let r = |include_stack: &mut lexer::IncludeStack| -> Result<Self> {
            let mut warnings = Warnings::new();

            let rented_script = srs::Script::try_new::<Error, _>(script.clone(), |script| {
                let cu = include_stack.push(file_name)?;
                let lexemes: Vec<_> = lexer::Preprocessor::preprocess(
                    module_path,
                    file_name,
                    script,
                    cu,
                    include_stack,
                )?;
                let filtered_tokens = lexemes
                    .into_iter()
                    .filter_map(Result::ok)
                    .filter(|t| !t.value.is_ignorable());

                let script_raw = grammar::ScriptParser::new().parse(filtered_tokens)?;
                let fake_aggr_reg = AggrRegistry::default();
                let mut helper = Helper::new(&reg, &fake_aggr_reg, include_stack.cus.clone());
                let screw_rust = script_raw.up_script(&mut helper)?;
                std::mem::swap(&mut warnings, &mut helper.warnings);
                Ok(screw_rust)
            })?;

            Ok(Self {
                script: rented_script,
                source: script,
                warnings,
            })
        }(&mut include_stack);
        r.map_err(|error| CompilerError {
            error,
            cus: include_stack.into_cus(),
        })
    }

    /// Returns the documentation for the script
    #[must_use]
    pub fn docs(&self) -> &Docs {
        &self.script.suffix().docs
    }

    /// Highlights a script with a given highlighter.
    /// # Errors
    /// on io errors
    #[cfg(not(tarpaulin_include))]
    pub fn highlight_script_with<H: Highlighter>(
        script: &str,
        h: &mut H,
        emit_lines: bool,
    ) -> io::Result<()> {
        let tokens: Vec<_> = lexer::Tokenizer::new(&script)
            .tokenize_until_err()
            .collect();
        h.highlight(None, &tokens, "", emit_lines, None)?;
        io::Result::Ok(())
    }

    /// Highlights a script range with a given highlighter and line indents.
    /// # Errors
    /// on io errors
    #[cfg(not(tarpaulin_include))]
    pub fn highlight_script_with_range<H: Highlighter>(
        script: &str,
        r: Range,
        h: &mut H,
    ) -> io::Result<()> {
        Self::highlight_script_with_range_indent("", script, r, h)
    }

    /// Highlights a script range with a given highlighter.
    /// # Errors
    /// on io errors
    #[cfg(not(tarpaulin_include))]
    pub fn highlight_script_with_range_indent<H: Highlighter>(
        line_prefix: &str,
        script: &str,
        r: Range,
        h: &mut H,
    ) -> io::Result<()> {
        let mut s = script.to_string();
        let mut include_stack = lexer::IncludeStack::default();
        let cu = include_stack.push("test.tremor")?;

        let tokens: Vec<_> = lexer::Preprocessor::preprocess(
            &crate::path::load(),
            "test.tremor",
            &mut s,
            cu,
            &mut include_stack,
        )?
        .into_iter()
        .filter_map(Result::ok)
        .collect();

        h.highlight(None, &tokens, line_prefix, true, Some(r))?;
        io::Result::Ok(())
    }

    /// Format an error given a script source.
    /// # Errors
    /// on io errors
    pub fn format_error_from_script<H: Highlighter>(
        script: &str,
        h: &mut H,
        CompilerError { error, cus }: &CompilerError,
    ) -> io::Result<()> {
        Self::format_error_from_script_and_cus(script, h, error, cus)
    }

    /// Format an error given a script source.
    fn format_error_from_script_and_cus<H: Highlighter>(
        script: &str,
        h: &mut H,
        error: &Error,
        cus: &[lexer::CompilationUnit],
    ) -> io::Result<()> {
        if let (Some(r), _) = error.context() {
            let cu = error.cu();
            if cu == 0 {
                // i wanna use map_while here, but it is still unstable :(
                let tokens: Vec<_> = lexer::Tokenizer::new(&script)
                    .tokenize_until_err()
                    .collect();
                h.highlight_error(None, &tokens, "", true, Some(r), Some(error.into()))?;
            } else if let Some(cu) = cus.get(cu) {
                let script = std::fs::read_to_string(cu.file_path())?;
                let tokens: Vec<_> = lexer::Tokenizer::new(&script)
                    .tokenize_until_err()
                    .collect();
                h.highlight_error(
                    cu.file_path().to_str(),
                    &tokens,
                    "",
                    true,
                    Some(r),
                    Some(error.into()),
                )?;
            } else {
                write!(h.get_writer(), "Error: {}", error)?;
            };
        } else {
            write!(h.get_writer(), "Error: {}", error)?;
        }
        h.finalize()
    }

    /// Format warnings with the given `Highligher`.
    /// # Errors
    /// on io errors
    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> io::Result<()> {
        for w in self.warnings() {
            let tokens: Vec<_> = lexer::Tokenizer::new(&self.source)
                .tokenize_until_err()
                .collect();
            h.highlight_error(None, &tokens, "", true, Some(w.outer), Some(w.into()))?;
        }
        h.finalize()
    }

    /// Formats an error within this script
    #[must_use]
    pub fn format_error(&self, e: &Error) -> String {
        let mut h = DumbHighlighter::default();
        if self.format_error_with(&mut h, &e).is_ok() {
            h.to_string()
        } else {
            format!("Failed to extract code for error: {}", e)
        }
    }

    /// Formats an error within this script using a given highlighter
    ///
    /// # Errors
    /// on io errors
    pub fn format_error_with<H: Highlighter>(&self, h: &mut H, e: &Error) -> io::Result<()> {
        let cus = &self.script.suffix().node_meta.cus;
        Self::format_error_from_script_and_cus(&self.source, h, e, cus)
    }

    /// Runs an event through this script
    ///
    /// # Errors
    /// if the script fails to run for the given context, event state and metadata
    pub fn run<'run, 'event>(
        &'event self,
        context: &'run EventContext,
        aggr: AggrType,
        event: &'run mut Value<'event>,
        state: &'run mut Value<'static>,
        meta: &'run mut Value<'event>,
    ) -> Result<Return<'event>>
    where
        'event: 'run,
    {
        self.script.suffix().run(context, aggr, event, state, meta)
    }
}
