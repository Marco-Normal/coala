use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

use miette::{miette, Diagnostic, Error, IntoDiagnostic};
use thiserror::Error;

use crate::col_parser::{ColConfig, ColType, DataValue};

#[derive(Debug)]
pub struct Csv {
    cols: Vec<ColType>,
    n_cols: usize,
    n_rows: usize,
    header: Vec<String>,
    cache: HashMap<String, Statistics>,
}

#[derive(Debug, Default)]
struct Statistics {
    mean: Option<DataValue>,
    median: Option<DataValue>,
    std_dev: Option<DataValue>,
}

pub struct ColViewer<'a> {
    pub(crate) inner: &'a ColType,
}

impl<'a> ColViewer<'a> {
    pub(crate) fn new(col: &'a ColType) -> Self {
        Self { inner: col }
    }
    pub(crate) fn get_ref(&self) -> &Self {
        self
    }
    pub fn name(&self) -> &str {
        self.inner.name()
    }
    pub fn mean(&self) -> Result<DataValue, Error> {
        self.inner.mean()
    }
    pub fn get(&self, index: usize) -> Result<DataValue, Error> {
        self.inner.data_as_value(index)
    }
    pub fn quantile(&self, quantile: f64) -> Result<DataValue, Error> {
        self.inner.quantile(quantile)
    }
    pub fn median(&self) -> Result<DataValue, Error> {
        self.inner.median()
    }
    pub fn mean_unchecked(&self) -> DataValue {
        self.inner.mean().unwrap()
    }
    pub fn get_unchecked(&self, index: usize) -> DataValue {
        self.inner.data_as_value(index).unwrap()
    }
    pub fn quantile_unchecked(&self, quantile: f64) -> DataValue {
        self.inner.quantile(quantile).unwrap()
    }
    pub fn median_unchecked(&self) -> DataValue {
        self.inner.median().unwrap()
    }
}

pub struct CsvConfig<'a> {
    pub separator: char,
    pub header: Option<usize>,
    pub parser_as_date: Option<HashMap<String, Option<&'a str>>>,
}
#[derive(Debug, Diagnostic, Error)]
enum ColParserError {
    #[error("Csv unexpectely ended")]
    UnexpectedEOF,
    #[error(
        "Number of lines to print (`{}`) is greater than DataFrame len (`{}`)",
        n,
        len
    )]
    OutOfLines { n: usize, len: usize },
    #[error("Column `{}` not found in Dataframe", name)]
    MissingCol { name: String },
    #[error(
        "Column `{}` doesn't have a datatype where `{}` can be calculated",
        name,
        metric
    )]
    InvalidMetric { name: String, metric: String },
}

macro_rules! statistics {
        ($($t:ident)*) => ($(
            pub fn $t(&mut self, name:&str) -> Result<DataValue, Error> {
                if let Some(cache) = self.cache.get(name) {
                    if let Some($t) = &cache.$t {
                       return Ok($t.clone());
                    }
                }

            let col = self
            .get_col(name)?;
        let $t = col.$t()?;
                self.cache.entry(col.name().to_string()).or_default().$t = Some($t.clone());
                Ok($t)
            }
        )*)
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
        let mut row_iters = values.into_iter().map(Vec::into_iter).collect::<Vec<_>>();
        let transposed: Vec<Vec<String>> = (0..n_cols)
            .map(|_| {
                row_iters
                    .iter_mut()
                    .map(|it| it.next().expect("Must exist from previous construction"))
                    .collect()
            })
            .collect();
        let n_rows = transposed[0].len();
        let mut cols: Vec<ColType> = Vec::with_capacity(n_cols);
        for (i, col_data) in transposed.into_iter().enumerate() {
            let col_name = header
                .get(i)
                .cloned()
                .unwrap_or_else(|| format!("Unnamed: {i}"));
            let config = if let Some(ref cols_as_date) = config.parser_as_date {
                if cols_as_date.contains_key(&col_name) {
                    Some(ColConfig {
                        as_date: true,
                        date_format: if let Some(&date_format) = cols_as_date.get(&col_name) {
                            date_format
                        } else {
                            None
                        },
                    })
                } else {
                    None
                }
            } else {
                None
            };
            cols.push(ColType::from_values(&col_data, col_name, config)?);
        }
        Ok(Self {
            cols,
            n_cols,
            n_rows,
            header,
            cache: Default::default(),
        })
    }
    fn print_n_lines(&self, beg: usize, end: usize) -> Result<String, Error> {
        if beg > self.n_rows {
            return Err(miette!(
                "Number of rows to print is greater than len of dataset"
            ));
        }
        let cols = self
            .cols
            .iter()
            .map(|col| col.print_range_lines(beg, end))
            .collect::<Result<Vec<_>, _>>()?;
        let (cols, mut max_widths): (Vec<_>, Vec<_>) = cols.into_iter().unzip();
        dbg!(&cols);
        let mut result = String::new();
        for (i, header) in self.header.iter().enumerate() {
            max_widths[i] = max_widths[i].max(self.header[i].len());
            result.push_str(&format!("{:<width$}", header, width = max_widths[i]));
            if i < self.n_cols - 1 {
                result.push_str(", ");
            }
        }
        result.push('\n');
        for row in cols.iter().take(beg - end) {
            for col_idx in 0..self.n_cols {
                let value = &row[col_idx];
                result.push_str(&format!("{:<width$}", value, width = max_widths[col_idx]));
                if col_idx < self.n_cols - 1 {
                    result.push_str(", ");
                }
            }
            result.push('\n')
        }
        Ok(result)
    }
    pub fn head(&self) -> Result<(), Error> {
        self.head_n(5)
    }
    pub fn head_n(&self, n_lines: usize) -> Result<(), Error> {
        if self.n_rows < n_lines {
            return Err(ColParserError::OutOfLines {
                n: n_lines,
                len: self.n_rows,
            }
            .into());
        }
        let result = self.print_n_lines(0, n_lines)?;
        println!("{result}");
        Ok(())
    }
    pub fn get_col(&self, name: &str) -> Result<ColViewer<'_>, Error> {
        self.cols
            .iter()
            .find(|c| c.name() == name)
            .map(ColViewer::new)
            .ok_or(
                ColParserError::MissingCol {
                    name: name.to_string(),
                }
                .into(),
            )
    }

    statistics! {mean median}
    pub fn quantile(&self, name: &str, quantile: f64) -> Result<DataValue, Error> {
        self.get_col(name)?.quantile(quantile)
    }
}
