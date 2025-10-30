use datetime::datetime::Datetime;
use log::info;
use miette::{miette, Diagnostic, Error, IntoDiagnostic};
use rand::{prelude::*, rng};
use std::{
    fmt::{self, Display},
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
    usize,
};
use thiserror::Error;

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
    pub(crate) fn mean(&self) -> Option<DataValue> {
        match self {
            Self::Float(col) => {
                let mean = col.values.iter().sum::<f64>();
                dbg!(mean);
                let mean = mean / col.values.len() as f64;
                Some(DataValue::Float(mean))
            }
            Self::Integer(col) => {
                let mean =
                    col.values.iter().map(|val| *val as f64).sum::<f64>() / col.values.len() as f64;
                Some(DataValue::Float(mean))
            }
            c => unreachable!("Col {c} doesn't have mean implemented"),
        }
    }
    pub(crate) fn data_as_value(&self, index: usize) -> Option<DataValue> {
        match self {
            ColType::Float(csv_col) => csv_col.values.get(index).map(|v| DataValue::Float(*v)),
            ColType::Integer(csv_col) => csv_col.values.get(index).map(|v| DataValue::Integer(*v)),
            ColType::String(csv_col) => csv_col
                .values
                .get(index)
                .map(|v| DataValue::String(v.clone())),
            ColType::Datetime(csv_col) => {
                csv_col.values.get(index).map(|v| DataValue::Datetime(*v))
            }
        }
    }
    pub(crate) fn quantile(&self, quantile: f64) -> Option<DataValue> {
        match self {
            ColType::Float(csv_col) => Some(DataValue::Float(csv_col.quantiles(quantile))),
            ColType::Integer(csv_col) => Some(DataValue::Integer(csv_col.quantiles(quantile))),
            ColType::Datetime(csv_col) => Some(DataValue::Datetime(csv_col.quantiles(quantile))),
            ColType::String(_) => unreachable!("String doesn't have a median"),
        }
    }
    pub(crate) fn median(&self) -> Option<DataValue> {
        self.quantile(0.50)
    }
}

impl<T: PartialOrd + Clone> CsvCol<T> {
    fn quantiles(&self, quantile: f64) -> T {
        _select_quantile(&self.values, quantile, |list| -> usize {
            let mut rng = rng();
            rng.random_range(0..list.len())
        })
    }
}

fn _select_quantile<T: PartialOrd + Clone>(
    list: &[T],
    quantile: f64,
    pivot_selection: fn(&[T]) -> usize,
) -> T {
    assert!(quantile > 0.0 && quantile < 1.0);
    let index = (1.0 / quantile) as usize;
    _quick_select(list, index, pivot_selection)
}

fn _quick_select<T: PartialOrd + Clone>(
    list: &[T],
    index: usize,
    pivot_selection: fn(&[T]) -> usize,
) -> T {
    if list.len() == 1 {
        assert_eq!(index, 0);
        return list[0].clone();
    }
    let pivot = pivot_selection(list);
    let pivot = &list[pivot];
    let lows: Vec<_> = list.iter().filter(|&x| x < pivot).cloned().collect();
    let highs: Vec<_> = list.iter().filter(|&x| x > pivot).cloned().collect();
    let pivots: Vec<_> = list.iter().filter(|&x| x == pivot).cloned().collect();
    if index < lows.len() {
        return _quick_select(&lows, index, pivot_selection);
    } else if index < lows.len() + pivots.len() {
        return pivots[0].clone();
    }
    _quick_select(&highs, index - lows.len() - pivots.len(), pivot_selection)
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

#[derive(Debug, Clone)]
pub enum DataValue {
    Float(f64),
    Integer(i64),
    String(String),
    Datetime(Datetime),
    Null,
}
