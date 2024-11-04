use crate::errors::PQRSError;
use crate::errors::PQRSError::CouldNotOpenFile;
use arrow::csv as csvwriter;
use arrow::json::{writer as jsonwriter, LineDelimitedWriter};
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use arrow_schema::ArrowError;
use log::debug;
use parquet::arrow::arrow_reader::{
    ArrowReaderBuilder, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fs::File;
use std::ops::Add;
use std::path::Path;
use walkdir::DirEntry;

// calculate the sizes in bytes for one KiB, MiB, GiB, TiB, PiB
static ONE_KI_B: i64 = 1024;
static ONE_MI_B: i64 = ONE_KI_B * 1024;
static ONE_GI_B: i64 = ONE_MI_B * 1024;
static ONE_TI_B: i64 = ONE_GI_B * 1024;
static ONE_PI_B: i64 = ONE_TI_B * 1024;

/// Output formats supported. Only cat command support CSV format.
#[derive(Copy, Clone, Debug)]
pub enum Formats {
    Default,
    Csv,
    CsvNoHeader,
    Json,
    NdJson,
}

impl std::fmt::Display for Formats {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Check if a particular path is present on the filesystem
pub fn check_path_present<P: AsRef<Path>>(file_path: P) -> bool {
    Path::new(file_path.as_ref()).exists()
}

/// Open the file based on the pat and return the File object, else return error
pub fn open_file<P: AsRef<Path>>(file_name: P) -> Result<File, PQRSError> {
    let file_name = file_name.as_ref();
    let path = Path::new(file_name);
    let file = match File::open(path) {
        Err(_) => return Err(CouldNotOpenFile(file_name.to_path_buf())),
        Ok(f) => f,
    };

    Ok(file)
}

/// Check if the given entry in the walking tree is a hidden file
pub fn _is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

/// ```
#[derive(Debug, Default)]
pub struct JsonValid {}

impl jsonwriter::JsonFormat for JsonValid {
    fn start_stream<W: std::io::Write>(&self, writer: &mut W) -> Result<(), ArrowError> {
        writer.write_all(b"[\n")?;
        Ok(())
    }

    fn start_row<W: std::io::Write>(
        &self,
        writer: &mut W,
        is_first_row: bool,
    ) -> Result<(), ArrowError> {
        if !is_first_row {
            writer.write_all(b",\n")?;
        }
        Ok(())
    }

    fn end_stream<W: std::io::Write>(&self, _writer: &mut W) -> Result<(), ArrowError> {
        _writer.write_all(b"\n]")?;
        Ok(())
    }
}

pub type ValidWriter<W> = jsonwriter::Writer<W, JsonValid>;

pub fn print_rows(
    file: File,
    num_records: Option<i64>,
    format: Formats,
) -> Result<(), PQRSError> {
    let out = std::io::stdout();
    let value = num_records.unwrap_or(0);
    let options = ArrowReaderOptions::new().with_page_index(value != 0);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    let reader;
    if value > 0 {
        reader = builder.with_limit(value as usize);
    } else if value < 0 {
        let total_rows = builder.metadata().file_metadata().num_rows();
        reader = builder.with_offset((total_rows + value) as usize);
    } else {
        reader = builder.with_batch_size(1024 * 8)
    }

    let mut stream = reader.build()?.into_iter().map(|x| x.unwrap());

    match format {
        Formats::Csv | Formats::CsvNoHeader => {
            let mut wtx = csvwriter::WriterBuilder::new().build(out);
            stream.try_for_each(|batch| wtx.write(&batch))?;
        }
        Formats::Default | Formats::NdJson => {
            let mut wtx = LineDelimitedWriter::new(out);
            stream.try_for_each(|batch| wtx.write(&batch))?;
        }
        Formats::Json => {
            let mut wtx = ValidWriter::new(out);
            stream
                .try_for_each(|batch| wtx.write(&batch))
                .and_then(|_| wtx.finish())?;
        }
    };
    Ok(())
}

/// Print the random sample of given size in either json or json-like format
pub fn print_rows_random(
    file: File,
    sample_size: usize,
    format: Formats,
) -> Result<(), PQRSError> {
    let parquet_reader = SerializedFileReader::new(file.try_clone()?)?;
    let iter = parquet_reader.get_row_iter(None)?;

    // find the number of records present in the file
    let total_records_in_file: i64 = get_row_count(file)?;
    // push all the indexes into the vector initially
    let mut indexes = (0..total_records_in_file).collect::<Vec<_>>();
    debug!("Original indexes: {:?}", indexes);

    // shuffle the indexes to randomize the vector
    let mut rng = thread_rng();
    indexes.shuffle(&mut rng);
    debug!("Shuffled indexes: {:?}", indexes);

    // take only the given number of records from the vector
    indexes = indexes.into_iter().take(sample_size).collect::<Vec<_>>();

    debug!("Sampled indexes: {:?}", indexes);

    for (start, row) in (0_i64..).zip(iter) {
        if indexes.contains(&start) {
            match row {
                Ok(rowval) => print_row(&rowval, format),
                Err(_) => todo!(),
            }
        }
    }

    Ok(())
}

/// A representation of Parquet file in a form that can be used for merging
#[derive(Debug)]
pub struct ParquetData {
    /// The schema of the parquet file
    pub schema: Schema,
    /// Collection of the record batches in the parquet file
    pub batches: Vec<RecordBatch>,
    /// The number of rows present in the parquet file
    pub rows: usize,
}

impl Add for ParquetData {
    type Output = Self;

    /// Combine two given parquet files
    fn add(mut self, mut rhs: Self) -> Self::Output {
        // the combined data contains data from both the structs
        let mut combined_data = Vec::new();
        combined_data.append(&mut self.batches);
        combined_data.append(&mut rhs.batches);

        Self {
            // the schema from the lhs is maintained, the assumption is that this
            // method is used only on files that share the same schema
            schema: self.schema,
            batches: combined_data,
            rows: self.rows + rhs.rows,
        }
    }
}

/// Return the row batches, rows and schema for a given parquet file
pub fn get_row_batches(file: File) -> Result<ParquetData, PQRSError> {
    let arrow_reader = ArrowReaderBuilder::try_new(file)?;

    let schema = Schema::clone(arrow_reader.schema());
    let record_batch_reader = arrow_reader.with_batch_size(1024).build()?;
    let mut batches: Vec<RecordBatch> = Vec::new();

    let mut rows = 0;
    for maybe_batch in record_batch_reader {
        let record_batch = maybe_batch?;
        rows += record_batch.num_rows();

        batches.push(record_batch);
    }

    Ok(ParquetData {
        schema,
        batches,
        rows,
    })
}

/// Print the given parquet rows in json or json-like format
fn print_row(row: &Row, format: Formats) {
    match format {
        Formats::NdJson => todo!(),
        Formats::Default => println!("{}", row),
        Formats::Csv => println!("Unsupported! {}", row),
        Formats::CsvNoHeader => println!("Unsupported! {}", row),
        Formats::Json => println!("{}", row.to_json_value()),
    }
}

/// Return the number of rows in the given parquet file
pub fn get_row_count(file: File) -> Result<i64, PQRSError> {
    let parquet_reader = SerializedFileReader::new(file)?;
    let row_group_metadata = parquet_reader.metadata().row_groups();
    // The parquet file is made up of blocks (also called row groups)
    // The row group metadata contains information about all the row groups present in the data
    // Each row group maintains the number of rows present in the block
    // Summing across all the row groups contains the total number of rows present in the file
    let total_num_rows = row_group_metadata.iter().map(|rg| rg.num_rows()).sum();

    Ok(total_num_rows)
}

/// Return the uncompressed and compressed size of the given file
pub fn get_size(file: File) -> Result<(i64, i64), PQRSError> {
    let parquet_reader = SerializedFileReader::new(file)?;
    let row_group_metadata = parquet_reader.metadata().row_groups();

    // Parquet format compresses data at a column level.
    // To calculate the size of the file (compressed or uncompressed), we need to sum
    // across all the row groups present in the parquet file. This is similar to how
    // we calculate the row count in the method above.
    // Do note that this size does not take the footer size into consideration.
    let uncompressed_size = row_group_metadata
        .iter()
        .map(|rg| rg.total_byte_size())
        .sum();
    let compressed_size = row_group_metadata
        .iter()
        .map(|rg| rg.compressed_size())
        .sum();

    Ok((uncompressed_size, compressed_size))
}

/// Pretty print the given size using human readable format
pub fn get_pretty_size(bytes: i64) -> String {
    if bytes / ONE_KI_B < 1 {
        return format!("{} Bytes", bytes);
    }

    if bytes / ONE_MI_B < 1 {
        return format!("{:.3} KiB", bytes / ONE_KI_B);
    }

    if bytes / ONE_GI_B < 1 {
        return format!("{:.3} MiB", bytes / ONE_MI_B);
    }

    if bytes / ONE_TI_B < 1 {
        return format!("{:.3} GiB", bytes / ONE_GI_B);
    }

    if bytes / ONE_PI_B < 1 {
        return format!("{:.3} TiB", bytes / ONE_TI_B);
    }

    format!("{:.3} PiB", bytes / ONE_PI_B)
}
