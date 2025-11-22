use datetime::datetime::Datetime;
use log::info;
use miette::{miette, Diagnostic, Error};
use std::{
    cell::RefCell,
    fmt::{self, Display},
    str::FromStr,
};
use thiserror::Error;

use crate::statistics::{Statistics, StatisticsError};

#[derive(Debug)]
pub(crate) struct CsvCol<T> {
    pub(crate) col_name: String,
    pub(crate) values: Vec<T>,
    pub(crate) n_elements: usize,
    pub(crate) sorted_values: RefCell<Option<(Vec<T>, usize)>>,
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
    #[error("Index out of range for column")]
    OutOfRange,
}
#[derive(Debug, Default)]
pub(crate) struct ColConfig<'a> {
    pub(crate) date_format: Option<&'a str>,
    pub(crate) as_date: bool,
}

impl fmt::Display for ColType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Float(col) => {
                writeln!(f, "{}", col.col_name)?;
                for idx in 0..col.n_elements {
                    writeln!(f, "{}", col.values[idx])?;
                }
                Ok(())
            }
            Self::Integer(col) => {
                writeln!(f, "{}", col.col_name)?;
                for idx in 0..col.n_elements {
                    writeln!(f, "{}", col.values[idx])?;
                }
                Ok(())
            }
            Self::String(col) => {
                writeln!(f, "{}", col.col_name)?;
                for idx in 0..col.n_elements {
                    writeln!(f, "{}", col.values[idx])?;
                }
                Ok(())
            }
            Self::Datetime(col) => {
                writeln!(f, "{}", col.col_name)?;
                for idx in 0..col.n_elements {
                    writeln!(f, "{}", col.values[idx])?;
                }
                Ok(())
            }
        }
    }
}

impl ColType {
    pub(crate) fn from_values(
        elements: &[String],
        name: String,
        config: Option<ColConfig>,
    ) -> Result<Self, Error> {
        if let Some(config) = config
           && let Some(col) = Self::as_date(elements, &name, config) {
                let col = col?;
                return Ok(Self::Datetime(col));
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
            } => Some(CsvCol::as_datetime(elements, name, date_format)),
            _ => None,
        }
    }
    pub(crate) fn print_range_lines(
        &self,
        beg: usize,
        end: usize,
    ) -> Result<(Vec<String>, usize), Error> {
        match self {
            Self::Float(col) => col.get_range_as_strings(beg, end),
            Self::Integer(col) => col.get_range_as_strings(beg, end),
            Self::Datetime(col) => col.get_range_as_strings(beg, end),
            Self::String(col) => col.get_range_as_strings(beg, end),
        }
    }
    pub(crate) fn name(&self) -> &str {
        match self {
            ColType::Float(csv_col) => &csv_col.col_name,
            ColType::Integer(csv_col) => &csv_col.col_name,
            ColType::String(csv_col) => &csv_col.col_name,
            ColType::Datetime(csv_col) => &csv_col.col_name,
        }
    }

    pub(crate) fn mean(&self) -> Result<DataValue, Error> {
        match self {
            Self::Float(col) => col.mean(),
            Self::Integer(col) => col.mean(),
            col => Err(StatisticsError::InvalidType {
                col: col.name().to_string(),
            }
            .into()),
        }
    }
    pub(crate) fn median(&self) -> Result<DataValue, Error> {
        match self {
            Self::Float(col) => col.median(),
            Self::Integer(col) => col.median(),
            col => Err(StatisticsError::InvalidType {
                col: col.name().to_string(),
            }
            .into()),
        }
    }
    pub(crate) fn quantile(&self, quantile: f64) -> Result<DataValue, Error> {
        match self {
            Self::Float(col) => col.quantile(quantile),
            Self::Integer(col) => col.quantile(quantile),
            col => Err(StatisticsError::InvalidType {
                col: col.name().to_string(),
            }
            .into()),
        }
    }
    pub(crate) fn stddev(&self) -> Result<DataValue, Error> {
        match self {
            Self::Float(col) => col.stddev(),
            Self::Integer(col) => col.stddev(),
            col => Err(StatisticsError::InvalidType {
                col: col.name().to_string(),
            }
            .into()),
        }
    }
    pub(crate) fn data_as_value(&self, index: usize) -> Result<DataValue, Error> {
        match self {
            ColType::Float(csv_col) => csv_col
                .values
                .get(index)
                .map(|f| DataValue::Float(*f))
                .ok_or(ColParseError::OutOfRange.into()),
            ColType::Integer(csv_col) => csv_col
                .values
                .get(index)
                .map(|f| DataValue::Integer(*f))
                .ok_or(ColParseError::OutOfRange.into()),
            ColType::String(csv_col) => csv_col
                .values
                .get(index)
                .map(|f| DataValue::String(f.clone()))
                .ok_or(ColParseError::OutOfRange.into()),
            ColType::Datetime(csv_col) => csv_col
                .values
                .get(index)
                .map(|f| DataValue::DateTime(*f))
                .ok_or(ColParseError::OutOfRange.into()),
        }
    }
}

impl<T: Display> CsvCol<T> {
    fn get_range_as_strings(&self, beg: usize, end: usize) -> Result<(Vec<String>, usize), Error> {
        if end > self.n_elements || beg > end {
            return Err(miette!("n is greater than number of lines in col"));
        }
        let mut max_width = 0;
        let mut strings = Vec::with_capacity(end - beg);
        for i in beg..end {
            let s = self.values[i].to_string();
            if s.len() > max_width {
                max_width = s.len();
            }
            strings.push(s);
        }
        Ok((strings, max_width))
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
            sorted_values: RefCell::default(),
        })
    }
}

// impl CsvCol<f64> {
//     fn get_sorted(&self) -> Vec<f64> {
//         if let Some((cached, len)) = &*self.sorted_values.borrow() {
//             if *len == self.n_elements {
//                 return cached.clone();
//             }
//         }
//         let mut sorted = self.values.clone();
//         sorted.sort_unstable_by(|a, b| a.total_cmp(b));
//         *self.sorted_values.borrow_mut() = Some((sorted.clone(), sorted.len()));
//         sorted
//     }
// }

impl<T: PartialOrd + Clone> CsvCol<T> {
    pub(crate) fn get_sorted(&self) -> Vec<T> {
        if let Some((cached, len)) = &*self.sorted_values.borrow() 
            && *len == self.n_elements {
                return cached.clone();
            }
        
        let mut sorted = self.values.clone();
        sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        *self.sorted_values.borrow_mut() = Some((sorted.clone(), sorted.len()));
        sorted
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
            sorted_values: RefCell::default(),
        })
    }
}

#[derive(Debug, Clone)]
pub enum DataValue {
    Float(f64),
    Integer(i64),
    Unsigned(u64),
    String(String),
    DateTime(Datetime),
    Null,
}
