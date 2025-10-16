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
struct CsvCol<T> {
    col_name: String,
    values: Vec<T>,
    n_elements: usize,
}
#[derive(Debug)]
enum CsvType {
    Float(CsvCol<f64>),
    Integer(CsvCol<i64>),
    String(CsvCol<String>),
    Datetime(CsvCol<Datetime>),
}
#[derive(Debug, Error, Diagnostic)]
enum CsvParseError {
    #[error("Error in collumn named `{col_name}`. Invalid data type, couldn't match with any.")]
    InvalidColType { col_name: String },
}
#[derive(Debug)]
struct CsvConfig {
    date_format: Option<String>,
    as_date: bool,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            date_format: None,
            as_date: false,
        }
    }
}

impl CsvType {
    fn from_path(
        path: &str,
        col_number: usize,
        separator: char,
        config: Option<CsvConfig>,
    ) -> Result<Self, Error> {
        if let Some(config) = config {
            if let Some(col) = Self::as_date(path, col_number, separator, config) {
                let col = col?;
                return Ok(Self::Datetime(col));
            }
        }
        let col_name = CsvHandler::get_col_name(path, col_number, separator)?;
        macro_rules! try_type {
            ($t:ty, $p:expr, $c:expr, $s:expr, $en:ident) => {
                match CsvCol::<$t>::from_path($p, $c, $s) {
                    Ok(col) => return Ok(CsvType::$en(col)),
                    Err(e) => info!(
                        "Column {} couldn't be parsed as type '{}'. Reason: {}",
                        $c,
                        stringify!($t),
                        e
                    ),
                }
            };
        }
        try_type!(i64, path, col_number, separator, Integer);
        try_type!(f64, path, col_number, separator, Float);
        try_type!(String, path, col_number, separator, String);
        Err(CsvParseError::InvalidColType { col_name }.into())
    }
    fn as_date(
        path: &str,
        col_number: usize,
        separator: char,
        config: CsvConfig,
    ) -> Option<Result<CsvCol<Datetime>, Error>> {
        match config {
            CsvConfig {
                date_format,
                as_date: true,
            } => Some(CsvCol::as_datetime(
                path,
                col_number,
                separator,
                date_format.as_deref(),
            )),
            _ => None,
        }
    }
}

impl<T: FromStr> CsvCol<T> {
    fn from_path(path: &str, col_number: usize, separator: char) -> Result<Self, Error> {
        let csv = File::open(path).into_diagnostic()?;
        let reader = BufReader::new(csv);
        let mut lines = reader.lines();
        let mut values: Vec<T> = Vec::new();
        let name = match lines.next() {
            Some(name) => {
                let name = name.into_diagnostic()?;
                let parts = name.split(separator).nth(col_number);
                if let Some(parts) = parts {
                    parts.to_string()
                } else {
                    String::from("Unnamed")
                }
            }
            None => String::from("Unnamed"),
        };
        for line in lines {
            if let Some(val) = line.into_diagnostic()?.split(separator).nth(col_number) {
                let t = match val.parse::<T>() {
                    Ok(t) => t,
                    Err(_) => {
                        return Err(miette!(
                            "Error parsing value `{val}`. String couldn't be converted safely."
                        ))
                    }
                };
                values.push(t);
            }
        }
        Ok(Self {
            col_name: name,
            n_elements: values.len(),
            values,
        })
    }
}
impl CsvCol<Datetime> {
    fn as_datetime(
        path: &str,
        col_number: usize,
        separator: char,
        format: Option<&str>,
    ) -> Result<CsvCol<Datetime>, Error> {
        let csv = File::open(path).into_diagnostic()?;
        let reader = BufReader::new(csv);
        let mut lines = reader.lines();
        let mut values: Vec<Datetime> = Vec::new();
        let name = match lines.next() {
            Some(name) => {
                let name = name.into_diagnostic()?;
                let parts = name.split(separator).nth(col_number);
                if let Some(parts) = parts {
                    parts.to_string()
                } else {
                    String::from("Unnamed")
                }
            }
            None => String::from("Unnamed"),
        };
        for line in lines {
            if let Some(val) = line.into_diagnostic()?.split(separator).nth(col_number) {
                let t: Datetime;
                if let Some(format) = format {
                    t = match Datetime::from_str(val, format) {
                        Ok(t) => t,
                        Err(e) => {
                            return Err(miette!(
                            "Error parsing value `{val}`. String couldn't be converted safely. {e}"
                        ))
                        }
                    };
                } else {
                    t = match Datetime::try_guess(val) {
                        Some(t) => t,
                        None => {
                            return Err(miette!(
                                "Error parsing value `{val}`. String couldn't be converted safely."
                            ))
                        }
                    };
                }
                values.push(t);
            }
        }
        Ok(CsvCol {
            col_name: name,
            n_elements: values.len(),
            values,
        })
    }
}
