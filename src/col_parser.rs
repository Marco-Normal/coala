use datetime::datetime::Datetime;
use log::info;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};
use thiserror::Error;

use miette::{miette, Diagnostic, Error, IntoDiagnostic};

struct CsvHandler;

impl CsvHandler {
    fn get_col_name(path: &str, col_number: usize, separator: char) -> Result<String, Error> {
        let csv = File::open(path).into_diagnostic()?;
        let reader = BufReader::new(csv);
        let header = reader.lines().next();
        match header {
            Some(name) => {
                let name = name.into_diagnostic()?;
                let parts = name.split(separator).nth(col_number);
                if let Some(parts) = parts {
                    Ok(parts.to_string())
                } else {
                    Ok(String::from("Unnamed"))
                }
            }
            None => Ok(String::from("Unnamed")),
        }
    }
}

#[derive(Debug)]
pub(crate) struct CsvCol<T> {
    col_name: String,
    values: Vec<T>,
    n_elements: usize,
}
#[derive(Debug)]
pub(crate) enum ColType {
    Float(CsvCol<f64>),
    Integer(CsvCol<i64>),
    String(CsvCol<String>),
    Datetime(CsvCol<Datetime>),
}
#[derive(Debug, Error, Diagnostic)]
enum ColParseError {
    #[error("Error in collumn named `{name}`. Invalid data type, couldn't match with any.")]
    InvalidColType { name: String },
}
#[derive(Debug)]
#[derive(Default)]
pub(crate) struct ColConfig {
    pub(crate) date_format: Option<String>,
    pub(crate) as_date: bool,
}


impl ColType {
    pub(crate) fn from_values(
        elements: &[String],
        name: String,
        config: Option<ColConfig>,
    ) -> Result<Self, Error> {
        if let Some(config) = config {
            if let Some(col) = Self::as_date(elements, &name, config) {
                let col = col?;
                return Ok(Self::Datetime(col));
            }
        }
        macro_rules! try_type {
            ($t:ty, $p:expr,  $n:expr, $en:ident) => {
                match CsvCol::<$t>::from_str_list($p, $n) {
                    Ok(col) => return Ok(ColType::$en(col)),
                    Err(e) => info!(
                        "Column {} couldn't be parsed as type '{}'. Reason: {}",
                        &$n,
                        stringify!($t),
                        e
                    ),
                }
            };
        }
        try_type!(i64, elements, &name, Integer);
        try_type!(f64, elements, &name, Float);
        try_type!(String, elements, &name, String);
        Err(ColParseError::InvalidColType { name }.into())
    }
    pub(crate) fn as_date(
        elements: &[String],
        name: &str,
        config: ColConfig,
    ) -> Option<Result<CsvCol<Datetime>, Error>> {
        match config {
            ColConfig {
                date_format,
                as_date: true,
            } => Some(CsvCol::as_datetime(elements, name, date_format.as_deref())),
            _ => None,
        }
    }
}

impl<T: FromStr> CsvCol<T> {
    fn from_str_list(elements: &[String], name: &str) -> Result<Self, Error> {
        let mut values: Vec<T> = Vec::new();
        for line in elements {
            let t = match line.parse::<T>() {
                Ok(t) => t,
                Err(_) => {
                    return Err(miette!(
                        "Error parsing value `{line}`. String couldn't be converted safely."
                    ))
                }
            };
            values.push(t);
        }
        Ok(Self {
            col_name: name.to_string(),
            n_elements: values.len(),
            values,
        })
    }
}
impl CsvCol<Datetime> {
    fn as_datetime(elements: &[String], name: &str, format: Option<&str>) -> Result<Self, Error> {
        let mut values = Vec::new();
        for line in elements {
            let t: Datetime;
            if let Some(format) = format {
                t = match Datetime::from_str(line, format) {
                    Ok(t) => t,
                    Err(e) => {
                        return Err(miette!(
                            "Error parsing value `{line}`. String couldn't be converted safely. {e}"
                        ))
                    }
                };
            } else {
                t = match Datetime::try_guess(line) {
                    Some(t) => t,
                    None => {
                        return Err(miette!(
                            "Error parsing value `{line}`. String couldn't be converted safely."
                        ))
                    }
                };
            }
            values.push(t);
        }
        Ok(CsvCol {
            col_name: name.to_string(),
            n_elements: values.len(),
            values,
        })
    }
}
