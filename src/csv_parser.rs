use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use miette::{miette, Diagnostic, Error, IntoDiagnostic};
use thiserror::Error;

use crate::col_parser::{ColConfig, ColType};

#[derive(Debug)]
pub struct Csv {
    cols: Vec<ColType>,
    n_cols: usize,
}

pub struct CsvConfig {
    pub separator: char,
    pub header: Option<usize>,
    pub parser_as_date: Option<Vec<String>>,
}
#[derive(Debug, Diagnostic, Error)]
enum ColParserError {
    #[error("Csv unexpectely ended")]
    UnexpectedEOF,
}
impl Csv {
    pub fn new(path: &str, config: CsvConfig) -> Result<Self, Error> {
        let csv = File::open(path).into_diagnostic()?;
        let reader = BufReader::new(csv);
        let mut lines = if let Some(header) = config.header {
            let mut lines = reader.lines();
            for _ in 0..header {
                lines.next();
            }
            lines
        } else {
            reader.lines()
        };
        let header: Vec<String> = match lines.next() {
            Some(header) => {
                let header = header.into_diagnostic()?;
                header
                    .split(config.separator)
                    .map(|s| s.to_string())
                    .collect()
            }
            None => return Err(ColParserError::UnexpectedEOF.into()),
        };
        let n_cols = header.len();
        let values: Vec<Vec<_>> = lines
            .map(|l| {
                let l = l.into_diagnostic();
                match l {
                    Ok(l) => Ok(l
                        .split(config.separator)
                        .map(|l| l.to_string())
                        .collect::<Vec<_>>()),
                    Err(e) => Err(miette!(e)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let n_rows = values.len();
        let mut row_iters = values.into_iter().map(Vec::into_iter).collect::<Vec<_>>();
        let transposed: Vec<Vec<String>> = (0..n_cols)
            .map(|_| {
                row_iters
                    .iter_mut()
                    .map(|it| it.next().expect("Must exist from previous construction"))
                    .collect()
            })
            .collect();
        dbg!(&transposed);
        let mut cols: Vec<ColType> = Vec::with_capacity(n_cols);
        for (i, col_data) in transposed.into_iter().enumerate() {
            let col_name = header
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("Unnamed: {i}"));
            if let Some(ref cols_as_date) = config.parser_as_date {
                if cols_as_date
                    .iter()
                    .any(|name| matches!(name, col_name))
                {
                    cols.push(ColType::from_values(
                        &col_data,
                        col_name,
                        Some(ColConfig {
                            as_date: true,
                            ..Default::default()
                        }),
                    )?);
                }
            } else {
                todo!()
            };
        }
        todo!()
    }
}
