use miette::{Diagnostic, Error};
use thiserror::Error;

use crate::col_parser::{CsvCol, DataValue};

pub trait Statistics {
    fn mean(&self) -> Result<DataValue, Error>;
    fn median(&self) -> Result<DataValue, Error>;
    fn quantile(&self, quantile: f64) -> Result<DataValue, Error>;
    fn stddev(&self) -> Result<DataValue, Error>;
}

#[derive(Error, Debug, Diagnostic)]
pub(crate) enum StatisticsError {
    #[error("Invalid quantile `{value}`, value must be between 0 and 1")]
    InvalidQuantile { value: f64 },
    #[error("Column cannot be empty")]
    EmptyColumn,
    #[error("`{col}` invalid for calculations")]
    InvalidType { col: String },
}

impl Statistics for CsvCol<f64> {
    fn mean(&self) -> Result<DataValue, Error> {
        if self.n_elements == 0 {
            return Err(StatisticsError::EmptyColumn.into());
        }
        let mean = self.values.iter().sum::<f64>();
        let mean = mean / self.values.len() as f64;
        Ok(DataValue::Float(mean))
    }
    fn median(&self) -> Result<DataValue, Error> {
        if self.n_elements == 0 {
            return Err(StatisticsError::EmptyColumn.into());
        }
        let col = self.get_sorted();
        if self.n_elements.is_multiple_of(2) {
            return Ok(DataValue::Float(col[col.len() / 2]));
        }
        Ok(DataValue::Float(
            0.5 * (col[col.len() / 2] + col[col.len() / 2 - 1]),
        ))
    }
    fn quantile(&self, quantile: f64) -> Result<DataValue, Error> {
        if !(0.0..1.0).contains(&quantile) {
            return Err(StatisticsError::InvalidQuantile { value: quantile }.into());
        }
        let col = self.get_sorted();
        let n = col.len();
        let index = quantile * (n - 1) as f64;
        let index = if index < 0.0 {
            0.0
        } else if index > (n - 1) as f64 {
            (n - 1) as f64
        } else {
            index
        };
        let lower_idx = index.floor() as usize;
        let upper_idx = lower_idx + 1;
        if upper_idx >= n {
            return Ok(DataValue::Float(col[lower_idx]));
        }
        let fraction = index - lower_idx as f64;
        let lower_val = col[lower_idx];
        let upper_val = col[upper_idx];
        let value = lower_val * (1.0 - fraction) + upper_val * fraction;
        Ok(DataValue::Float(value))
    }
    fn stddev(&self) -> Result<DataValue, Error> {
        todo!()
    }
}

impl Statistics for CsvCol<i64> {
    fn mean(&self) -> Result<DataValue, Error> {
        if self.n_elements == 0 {
            return Err(StatisticsError::EmptyColumn.into());
        }
        let sum: f64 = self.values.iter().map(|&x| x as f64).sum();
        Ok(DataValue::Float(sum / self.n_elements as f64))
    }

    fn median(&self) -> Result<DataValue, Error> {
        if self.n_elements == 0 {
            return Err(StatisticsError::EmptyColumn.into());
        }
        let mut col = self.values.to_vec();
        col.sort_unstable();
        if !self.n_elements.is_multiple_of(2) {
            return Ok(DataValue::Integer(col[col.len() / 2]));
        };
        Ok(DataValue::Integer(
            col[col.len() / 2] / 2 + col[col.len() / 2 - 1],
        ))
    }

    fn quantile(&self, quantile: f64) -> Result<DataValue, Error> {
        if !(0.0..1.0).contains(&quantile) {
            return Err(StatisticsError::InvalidQuantile { value: quantile }.into());
        }
        let col = self.get_sorted();
        let n = col.len();
        let index = (quantile * n as f64).ceil() as usize - 1;
        let index = if index == usize::MAX {
            0
        } else if index >= n {
            n - 1
        } else {
            index
        };
        Ok(DataValue::Integer(col[index]))
    }

    fn stddev(&self) -> Result<DataValue, Error> {
        todo!()
    }
}
