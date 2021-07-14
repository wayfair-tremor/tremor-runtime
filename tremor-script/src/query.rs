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

use crate::ast::{self, Warning};
use crate::errors::{CompilerError, Error, Result};
use crate::highlighter::{Dumb as DumbHighlighter, Highlighter};
use crate::path::ModulePath;
use crate::prelude::*;
use crate::{lexer, srs};
use std::collections::BTreeSet;
use std::io::Write;

/// A tremor query
#[derive(Debug, Clone)]
pub struct Query {
    /// The query
    pub query: srs::Query,
    /// Source of the query
    pub source: String,
    /// Warnings emitted by the script
    pub warnings: BTreeSet<Warning>,
    /// Number of local variables (should be 0)
    pub locals: usize,
}

impl<'run, 'event, 'script> Query
where
    'script: 'event,
    'event: 'run,
{
    /// Extracts SRS  statements
    #[must_use]
    pub fn extract_stmts(&self) -> Vec<srs::Stmt> {
        self.query.extract_stmts()
    }
    /// Borrows the query
    #[must_use]
    pub fn suffix(&self) -> &ast::Query {
        self.query.suffix()
    }
    /// Parses a string into a query
    ///
    /// # Errors
    /// if the query can not be parsed
    pub fn parse(
        module_path: &ModulePath,
        file_name: &str,
        script: &'script str,
        cus: Vec<ast::CompilationUnit>,
        reg: &Registry,
        aggr_reg: &AggrRegistry,
    ) -> std::result::Result<Self, CompilerError> {
        let mut source = script.to_string();

        let mut warnings = BTreeSet::new();
        let mut locals = 0;

        // TODO make lexer EOS tolerant to avoid this kludge
        source.push('\n');

        let mut include_stack = lexer::IncludeStack::default();

        let r = |include_stack: &mut lexer::IncludeStack| -> Result<Self> {
            let query = srs::Query::try_new::<Error, _>(source.clone(), |src: &mut String| {
                let mut helper = ast::Helper::new(reg, aggr_reg, cus);
                let cu = include_stack.push(&file_name)?;
                let lexemes: Vec<_> = lexer::Preprocessor::preprocess(
                    module_path,
                    file_name,
                    src,
                    cu,
                    include_stack,
                )?;
                let filtered_tokens = lexemes
                    .into_iter()
                    .filter_map(Result::ok)
                    .filter(|t| !t.value.is_ignorable());
                let script_stage_1 = crate::parser::g::QueryParser::new().parse(filtered_tokens)?;
                let script = script_stage_1.up_script(&mut helper)?;

                std::mem::swap(&mut warnings, &mut helper.warnings);
                locals = helper.locals.len();
                Ok(script)
            })?;

            Ok(Self {
                query,
                source,
                warnings,
                locals,
            })
        }(&mut include_stack);
        r.map_err(|error| CompilerError {
            error,
            cus: include_stack.into_cus(),
        })
    }

    /// Highlights a script with a given highlighter.
    /// # Errors
    /// on io errors
    #[cfg(not(tarpaulin_include))]
    pub fn highlight_script_with<H: Highlighter>(
        script: &str,
        h: &mut H,
        emit_lines: bool,
    ) -> std::io::Result<()> {
        let mut script = script.to_string();
        script.push('\n');
        let tokens: Vec<_> = lexer::Tokenizer::new(&script)
            .tokenize_until_err()
            .collect();
        h.highlight(None, &tokens, "", emit_lines, None)
    }

    /// Format an error given a script source.
    /// # Errors
    /// on io errors
    pub fn format_error_from_script<H: Highlighter>(
        script: &str,
        h: &mut H,
        e: &Error,
    ) -> std::io::Result<()> {
        let mut script = script.to_string();
        script.push('\n');

        let tokens: Vec<_> = lexer::Tokenizer::new(&script)
            .tokenize_until_err()
            .collect();
        match e.context() {
            (Some(r), _) => {
                h.highlight_error(None, &tokens, "", true, Some(r), Some(e.into()))?;
                h.finalize()
            }

            _other => {
                write!(h.get_writer(), "Error: {}", e)?;
                h.finalize()
            }
        }
    }

    /// Format an error given a script source.
    /// # Errors
    /// on io errors
    pub fn format_warnings_with<H: Highlighter>(&self, h: &mut H) -> std::io::Result<()> {
        for w in &self.warnings {
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
    pub fn format_error_with<H: Highlighter>(&self, h: &mut H, e: &Error) -> std::io::Result<()> {
        Self::format_error_from_script(&self.source, h, e)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn parse(query: &str) {
        let reg = crate::registry();
        let aggr_reg = crate::aggr_registry();
        let module_path = crate::path::load();
        let cus = vec![];
        if let Err(e) = Query::parse(&module_path, "test.trickle", query, cus, &reg, &aggr_reg) {
            eprintln!("{}", e.error());
            assert!(false)
        } else {
            assert!(true)
        }
    }
    #[test]
    fn for_in_select() {
        parse(r#"select for event of case (a, b) => b end from in into out;"#);
    }

    #[test]
    fn script_with_args() {
        parse(
            r#"
define script test
with
  beep = "beep"
script
  { "beep": "{args.beep}" }
end;

create script beep from test;
create script boop from test
with
  beep = "boop" # override
end;

# Stream ingested data into script with default params
select event from in into beep;

# Stream ingested data into script with overridden params
select event from in into boop;

# Stream script operator synthetic events into out stream
select event from beep into out;
select event from boop into out;
        "#,
        )
    }
}
