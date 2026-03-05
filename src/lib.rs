use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow::array::*;
use arrow::record_batch::RecordBatch;
use chrono::Datelike;
use memmap2::Mmap;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBFError {
    #[error("File not found: {0}")]
    FileNotFound(String),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("Invalid file format")]
    InvalidFormat,
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("CSV error: {0}")]
    CsvError(#[from] csv::Error),
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("Parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),
}

impl From<DBFError> for PyErr {
    fn from(err: DBFError) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    Char,
    Date,
    Float,
    Numeric,
    Logical,
    Memo,
}

impl FieldType {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'C' => Some(Self::Char),
            b'D' => Some(Self::Date),
            b'F' => Some(Self::Float),
            b'N' => Some(Self::Numeric),
            b'L' => Some(Self::Logical),
            b'M' => Some(Self::Memo),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FieldDescriptor {
    pub name: String,
    pub field_type: FieldType,
    pub length: u8,
    pub decimal_count: u8,
    pub offset: usize,
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct DBF {
    #[pyo3(get)]
    pub rows: u32,
    #[pyo3(get)]
    pub cols: u32,
    fields: Vec<FieldDescriptor>,
    record_length: usize,
    data: Arc<Mmap>,
    header_length: usize,
}

#[pymethods]
impl DBF {
    #[staticmethod]
    #[pyo3(name = "read")]
    pub fn py_read(path: String) -> PyResult<Self> {
        Ok(Self::read_file(path)?)
    }

    #[pyo3(name = "field_names")]
    pub fn py_field_names(&self) -> Vec<String> {
        self.fields.iter().map(|f| f.name.clone()).collect()
    }

    #[pyo3(name = "get_column")]
    pub fn py_get_column(&self, name: String) -> PyResult<Column> {
        Ok(self.get_column(&name)?)
    }

    #[pyo3(name = "to_csv")]
    pub fn py_to_csv(&self, path: String) -> PyResult<()> {
        Ok(self.to_csv_file(path)?)
    }

    #[pyo3(name = "to_parquet")]
    pub fn py_to_parquet(&self, path: String) -> PyResult<()> {
        Ok(self.to_parquet(path)?)
    }
}

impl DBF {
    pub fn read_file<P: AsRef<Path>>(path: P) -> Result<Self, DBFError> {
        let path = path.as_ref();
        let file =
            File::open(path).map_err(|_| DBFError::FileNotFound(path.display().to_string()))?;
        let mmap = unsafe { Mmap::map(&file)? };

        if mmap.len() < 32 {
            return Err(DBFError::InvalidFormat);
        }

        // Check for valid header marker
        if mmap[0] != 0x03 && mmap[0] != 0x30 {
            return Err(DBFError::InvalidFormat);
        }

        let rows = u32::from_le_bytes([mmap[4], mmap[5], mmap[6], mmap[7]]);
        let header_length = u16::from_le_bytes([mmap[8], mmap[9]]) as usize;
        let record_length = u16::from_le_bytes([mmap[10], mmap[11]]) as usize;

        let mut fields = Vec::new();
        let mut offset = 32;

        while offset < header_length - 1 {
            if mmap[offset] == 0x0D {
                break;
            }
            if offset + 32 > mmap.len() {
                return Err(DBFError::InvalidFormat);
            }

            let name_bytes = &mmap[offset..offset + 11];
            let name = String::from_utf8_lossy(name_bytes)
                .split('\0')
                .next()
                .unwrap_or("")
                .to_string();
            let field_type_byte = mmap[offset + 11];
            let field_type =
                FieldType::from_byte(field_type_byte).ok_or(DBFError::InvalidFormat)?;
            let length = mmap[offset + 16];
            let decimal_count = mmap[offset + 17];

            fields.push(FieldDescriptor {
                name,
                field_type,
                length,
                decimal_count,
                offset: 0,
            });
            offset += 32;
        }

        let mut current_offset = 1; // Start at 1 to skip the delete flag
        for field in &mut fields {
            field.offset = current_offset;
            current_offset += field.length as usize;
        }

        Ok(Self {
            rows,
            cols: fields.len() as u32,
            fields,
            record_length,
            data: Arc::new(mmap),
            header_length,
        })
    }

    pub fn write_file<P: AsRef<Path>>(&self, path: P) -> Result<(), DBFError> {
        let mut file = File::create(path)?;
        self.write_to(&mut file)
    }

    pub fn write_to<W: Write + std::io::Seek>(&self, writer: &mut W) -> Result<(), DBFError> {
        writer.write_all(&[0x03])?;
        let now = chrono::Local::now();
        let (year, month, day) = (now.year() as u8 - 100, now.month() as u8, now.day() as u8);
        writer.write_all(&[year, month, day])?;
        writer.write_all(&self.rows.to_le_bytes())?;
        let header_len = (32 + self.fields.len() * 32 + 1) as u16;
        writer.write_all(&header_len.to_le_bytes())?;
        writer.write_all(&(self.record_length as u16).to_le_bytes())?;
        writer.write_all(&[0; 16])?;

        for field in &self.fields {
            let mut name = field.name.as_bytes().to_vec();
            name.resize(11, 0);
            writer.write_all(&name)?;
            match field.field_type {
                FieldType::Char => writer.write_all(b"C")?,
                FieldType::Date => writer.write_all(b"D")?,
                FieldType::Float => writer.write_all(b"F")?,
                FieldType::Numeric => writer.write_all(b"N")?,
                FieldType::Logical => writer.write_all(b"L")?,
                FieldType::Memo => writer.write_all(b"M")?,
            }
            writer.write_all(&[0; 4])?; // Reserved bytes (offsets 12-15)
            writer.write_all(&[field.length, field.decimal_count])?; // Length and decimal (offsets 16-17)
            writer.write_all(&[0; 14])?; // More reserved bytes (offsets 18-31)
        }

        writer.write_all(&[0x0D])?;

        for i in 0..self.rows as usize {
            let record_start = self.header_length + i * self.record_length;
            let record_end = record_start + self.record_length;
            if record_end > self.data.len() {
                break;
            }
            writer.write_all(&self.data[record_start..record_end])?;
        }

        writer.write_all(&[0x1A])?;
        Ok(())
    }

    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }

    pub fn get_column(&self, name: &str) -> Result<Column, DBFError> {
        let field = self
            .fields
            .iter()
            .find(|f| f.name == name)
            .ok_or_else(|| DBFError::ColumnNotFound(name.to_string()))?;

        let mut values = Vec::with_capacity(self.rows as usize);

        for i in 0..self.rows as usize {
            let record_start = self.header_length + i * self.record_length;
            let record_end = record_start + self.record_length;
            if record_end > self.data.len() {
                break;
            }

            // Skip deleted records
            if self.data[record_start] == b'*' {
                continue;
            }

            let field_start = record_start + field.offset;
            let field_end = field_start + field.length as usize;
            let raw_value = &self.data[field_start..field_end];

            let value = match field.field_type {
                FieldType::Char => {
                    let trimmed = String::from_utf8_lossy(raw_value).trim().to_string();
                    if trimmed.is_empty() {
                        Value::Null
                    } else {
                        Value::String(trimmed)
                    }
                }
                FieldType::Numeric | FieldType::Float => {
                    let raw = String::from_utf8_lossy(raw_value).to_string();
                    if raw.trim().is_empty() {
                        Value::Null
                    } else {
                        Value::String(raw.trim().to_string())
                    }
                }
                FieldType::Date => {
                    let s = String::from_utf8_lossy(raw_value).trim().to_string();
                    if s.is_empty() || s == "00000000" {
                        Value::Null
                    } else {
                        Value::String(s)
                    }
                }
                FieldType::Logical => {
                    let c = raw_value.first().copied().unwrap_or(b' ');
                    match c {
                        b'Y' | b'y' | b'T' | b't' => Value::String("true".to_string()),
                        b'N' | b'n' | b'F' | b'f' => Value::String("false".to_string()),
                        _ => Value::Null,
                    }
                }
                FieldType::Memo => {
                    Value::String(String::from_utf8_lossy(raw_value).trim().to_string())
                }
            };
            values.push(value);
        }

        Ok(Column {
            name: name.to_string(),
            values,
            field_type: field.field_type.clone(),
        })
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch, DBFError> {
        let mut arrays: Vec<Arc<dyn Array>> = Vec::new();
        let mut field_names: Vec<String> = Vec::new();

        for field in &self.fields {
            let col = self.get_column(&field.name)?;
            let arrow_array = col.to_arrow_array()?;
            arrays.push(arrow_array);
            field_names.push(field.name.clone());
        }

        let fields_vec: Vec<arrow::datatypes::Field> = field_names
            .iter()
            .zip(arrays.iter())
            .map(|(name, arr)| arrow::datatypes::Field::new(name, arr.data_type().clone(), true))
            .collect();
        let schema = arrow::datatypes::Schema::new(fields_vec);

        Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
    }

    pub fn to_csv<W: Write>(&self, mut writer: W) -> Result<(), DBFError> {
        let mut csv_writer = csv::Writer::from_writer(&mut writer);

        let headers: Vec<String> = self.fields.iter().map(|f| f.name.clone()).collect();
        csv_writer.write_record(&headers)?;

        for i in 0..self.rows as usize {
            let record_start = self.header_length + i * self.record_length;
            let record_end = record_start + self.record_length;
            if record_end > self.data.len() {
                break;
            }

            // Skip deleted records
            if self.data[record_start] == b'*' {
                continue;
            }

            let mut record: Vec<String> = Vec::new();
            for field in &self.fields {
                let field_start = record_start + field.offset;
                let field_end = field_start + field.length as usize;
                let raw_value = &self.data[field_start..field_end];
                let value = String::from_utf8_lossy(raw_value).trim().to_string();
                record.push(value);
            }
            csv_writer.write_record(&record)?;
        }

        csv_writer.flush()?;
        Ok(())
    }

    pub fn to_csv_file<P: AsRef<Path>>(&self, path: P) -> Result<(), DBFError> {
        let file = File::create(path)?;
        self.to_csv(file)
    }

    pub fn to_parquet<P: AsRef<Path>>(&self, path: P) -> Result<(), DBFError> {
        let batch = self.to_record_batch()?;
        let schema = batch.schema();

        let file = File::create(path)?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    pub fn from_csv<P: AsRef<Path>>(path: P) -> Result<Self, DBFError> {
        let file = File::open(path)?;
        let mut csv_reader = csv::Reader::from_reader(file);

        let headers: Vec<String> = csv_reader
            .headers()?
            .iter()
            .map(|s| s.to_string())
            .collect();

        let mut records: Vec<Vec<String>> = Vec::new();
        for result in csv_reader.records() {
            let record = result?.iter().map(|s| s.to_string()).collect();
            records.push(record);
        }

        Self::from_data(&headers, &records)
    }

    pub fn from_data(headers: &[String], data: &[Vec<String>]) -> Result<Self, DBFError> {
        let rows = data.len() as u32;
        let cols = headers.len() as u32;

        let mut fields: Vec<FieldDescriptor> = Vec::new();
        let mut record_length = 1;

        for (i, header) in headers.iter().enumerate() {
            let max_len = data
                .iter()
                .map(|r| r.get(i).map(|s| s.len()).unwrap_or(0))
                .max()
                .unwrap_or(1)
                .max(header.len());

            let (field_type, length) = if data.iter().all(|r| {
                r.get(i)
                    .map(|s| s.trim().parse::<f64>().is_ok())
                    .unwrap_or(false)
            }) && !data.is_empty()
            {
                (FieldType::Numeric, max_len.min(20).max(1) as u8)
            } else {
                (FieldType::Char, max_len.min(254).max(1) as u8)
            };

            fields.push(FieldDescriptor {
                name: header.clone(),
                field_type,
                length,
                decimal_count: 0,
                offset: record_length, // Starts at 1 (after delete flag) because record_length starts at 1
            });
            record_length += length as usize;
        }

        let header_len = 32 + fields.len() * 32 + 1;
        let total_size = header_len + record_length * rows as usize + 1;
        let mut file_data = vec![0u8; total_size];

        file_data[0] = 0x03;
        let now = chrono::Local::now();
        file_data[1] = (now.year() - 1900) as u8;
        file_data[2] = now.month() as u8;
        file_data[3] = now.day() as u8;
        file_data[4..8].copy_from_slice(&rows.to_le_bytes());
        let hlen = header_len as u16;
        file_data[8..10].copy_from_slice(&hlen.to_le_bytes());
        file_data[10..12].copy_from_slice(&(record_length as u16).to_le_bytes());

        let mut offset = 32;
        for field in &fields {
            let name_bytes = field.name.as_bytes();
            file_data[offset..offset + name_bytes.len()].copy_from_slice(name_bytes);
            offset += 11;
            file_data[offset] = match field.field_type {
                FieldType::Char => b'C',
                FieldType::Numeric => b'N',
                FieldType::Float => b'F',
                FieldType::Date => b'D',
                FieldType::Logical => b'L',
                FieldType::Memo => b'M',
            };
            offset += 1; // Now at offset 12 (reserved bytes)
            file_data[offset..offset + 4].copy_from_slice(&[0u8; 4]); // Reserved bytes 12-15
            offset += 4; // Now at offset 16
            file_data[offset] = field.length;
            file_data[offset + 1] = field.decimal_count;
            offset += 14; // Reserved bytes 18-31
        }
        file_data[offset] = 0x0D;

        for (row_idx, row) in data.iter().enumerate() {
            let record_start = header_len + row_idx * record_length;
            file_data[record_start] = b' ';

            for (col_idx, field) in fields.iter().enumerate() {
                let value = row.get(col_idx).map(|s| s.as_str()).unwrap_or("");
                let field_start = record_start + field.offset;
                let value_bytes = value.as_bytes();
                let copy_len = value_bytes.len().min(field.length as usize);
                file_data[field_start..field_start + copy_len]
                    .copy_from_slice(&value_bytes[..copy_len]);
            }
        }
        file_data[header_len + record_length * rows as usize] = 0x1A;

        let mut file = File::create("/tmp/dbf_from_data.bin")?;
        file.write_all(&file_data)?;
        drop(file);
        let file = File::open("/tmp/dbf_from_data.bin")?;
        let mmap = unsafe { Mmap::map(&file)? };

        Ok(Self {
            rows,
            cols,
            fields,
            record_length,
            data: Arc::new(mmap),
            header_length: header_len,
        })
    }
}

impl std::ops::Index<&str> for DBF {
    type Output = Result<Value, DBFError>;

    fn index(&self, name: &str) -> &Self::Output {
        static NULL: std::sync::LazyLock<Result<Value, DBFError>> =
            std::sync::LazyLock::new(|| Err(DBFError::ColumnNotFound("".to_string())));

        match self.get_column(name) {
            Ok(col) => {
                if let Some(_val) = col.values.first() {
                    static OK: std::sync::LazyLock<Result<Value, DBFError>> =
                        std::sync::LazyLock::new(|| Ok(Value::String("".to_string())));
                    return &OK;
                }
            }
            Err(_) => {}
        }
        &NULL
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct Column {
    #[pyo3(get)]
    pub name: String,
    pub values: Vec<Value>,
    field_type: FieldType,
}

#[pymethods]
impl Column {
    #[pyo3(name = "sum")]
    pub fn py_sum(&self) -> Option<f64> {
        self.sum()
    }

    #[pyo3(name = "mean")]
    pub fn py_mean(&self) -> Option<f64> {
        self.mean()
    }

    #[pyo3(name = "min")]
    pub fn py_min(&self) -> Option<String> {
        self.min()
    }

    #[pyo3(name = "max")]
    pub fn py_max(&self) -> Option<String> {
        self.max()
    }

    #[pyo3(name = "count")]
    pub fn py_count(&self) -> usize {
        self.count()
    }

    #[pyo3(name = "unique")]
    pub fn py_unique(&self) -> Vec<String> {
        self.unique()
    }

    pub fn to_list(&self) -> Vec<Option<String>> {
        self.values
            .iter()
            .map(|v| match v {
                Value::String(s) => Some(s.clone()),
                Value::Null => None,
            })
            .collect()
    }
}

impl Column {
    pub fn to_arrow_array(&self) -> Result<Arc<dyn Array>, DBFError> {
        match self.field_type {
            FieldType::Char | FieldType::Date | FieldType::Logical | FieldType::Memo => {
                let values: Vec<&str> = self
                    .values
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => s.as_str(),
                        Value::Null => "",
                    })
                    .collect();
                Ok(Arc::new(StringArray::from(values)))
            }
            FieldType::Numeric | FieldType::Float => {
                let values: Vec<Option<f64>> = self
                    .values
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => s.trim().parse::<f64>().ok(),
                        Value::Null => None,
                    })
                    .collect();
                Ok(Arc::new(Float64Array::from(values)))
            }
        }
    }

    pub fn sum(&self) -> Option<f64> {
        match self.field_type {
            FieldType::Numeric | FieldType::Float => {
                let values = &self.values;
                let len = values.len();
                if len == 0 {
                    return Some(0.0);
                }

                // Pre-allocate array for better memory layout and auto-vectorization
                let mut buffer = Vec::with_capacity(len);
                for v in values {
                    if let Value::String(s) = v {
                        buffer.push(s.trim().parse::<f64>().unwrap_or(0.0));
                    }
                }

                // Use iterator sum with explicit addition order for better vectorization
                let mut sum = 0.0;
                let buffer_len = buffer.len();
                let num_chunks = buffer_len / 4;

                for chunk_idx in 0..num_chunks {
                    let idx = chunk_idx * 4;
                    sum += buffer[idx];
                    sum += buffer[idx + 1];
                    sum += buffer[idx + 2];
                    sum += buffer[idx + 3];
                }
                for i in num_chunks * 4..buffer_len {
                    sum += buffer[i];
                }
                Some(sum)
            }
            _ => None,
        }
    }

    pub fn mean(&self) -> Option<f64> {
        let s = self.sum()?;
        let count = self
            .values
            .iter()
            .filter(|v| !matches!(v, Value::Null))
            .count();
        if count > 0 {
            Some(s / count as f64)
        } else {
            None
        }
    }

    pub fn numeric_min(&self) -> Option<f64> {
        match self.field_type {
            FieldType::Numeric | FieldType::Float => {
                let values = &self.values;
                let mut min = f64::MAX;
                let mut found = false;

                let parsed: Vec<f64> = values
                    .iter()
                    .filter_map(|v| {
                        if let Value::String(s) = v {
                            found = true;
                            s.trim().parse::<f64>().ok()
                        } else {
                            None
                        }
                    })
                    .collect();

                if !found {
                    return None;
                }

                let parsed_len = parsed.len();
                let num_chunks = parsed_len / 4;

                for chunk_idx in 0..num_chunks {
                    let idx = chunk_idx * 4;
                    min = min.min(parsed[idx]);
                    min = min.min(parsed[idx + 1]);
                    min = min.min(parsed[idx + 2]);
                    min = min.min(parsed[idx + 3]);
                }
                for i in num_chunks * 4..parsed_len {
                    min = min.min(parsed[i]);
                }
                Some(min)
            }
            _ => None,
        }
    }

    pub fn numeric_max(&self) -> Option<f64> {
        match self.field_type {
            FieldType::Numeric | FieldType::Float => {
                let values = &self.values;
                let mut max = f64::MIN;
                let mut found = false;

                let parsed: Vec<f64> = values
                    .iter()
                    .filter_map(|v| {
                        if let Value::String(s) = v {
                            found = true;
                            s.trim().parse::<f64>().ok()
                        } else {
                            None
                        }
                    })
                    .collect();

                if !found {
                    return None;
                }

                let parsed_len = parsed.len();
                let num_chunks = parsed_len / 4;

                for chunk_idx in 0..num_chunks {
                    let idx = chunk_idx * 4;
                    max = max.max(parsed[idx]);
                    max = max.max(parsed[idx + 1]);
                    max = max.max(parsed[idx + 2]);
                    max = max.max(parsed[idx + 3]);
                }
                for i in num_chunks * 4..parsed_len {
                    max = max.max(parsed[i]);
                }
                Some(max)
            }
            _ => None,
        }
    }

    pub fn min(&self) -> Option<String> {
        let mut min: Option<String> = None;
        for v in &self.values {
            if let Value::String(s) = v {
                if s.trim().is_empty() {
                    continue;
                }
                match &min {
                    Some(m) => {
                        if s < m {
                            min = Some(s.clone());
                        }
                    }
                    None => min = Some(s.clone()),
                }
            }
        }
        min
    }

    pub fn max(&self) -> Option<String> {
        let mut max: Option<String> = None;
        for v in &self.values {
            if let Value::String(s) = v {
                if s.trim().is_empty() {
                    continue;
                }
                match &max {
                    Some(m) => {
                        if s > m {
                            max = Some(s.clone());
                        }
                    }
                    None => max = Some(s.clone()),
                }
            }
        }
        max
    }

    pub fn count(&self) -> usize {
        self.values
            .iter()
            .filter(|v| !matches!(v, Value::Null))
            .count()
    }

    pub fn unique(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for v in &self.values {
            if let Value::String(s) = v {
                if !s.trim().is_empty() && seen.insert(s.clone()) {
                    result.push(s.clone());
                }
            }
        }
        result
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Null,
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::Null => write!(f, ""),
        }
    }
}

impl From<Value> for String {
    fn from(v: Value) -> Self {
        match v {
            Value::String(s) => s,
            Value::Null => String::new(),
        }
    }
}

#[pymodule]
fn dbfbucket(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<DBF>()?;
    m.add_class::<Column>()?;
    Ok(())
}
