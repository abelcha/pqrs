use crate::errors::PQRSError;
use crate::utils::Formats;
use crate::utils::{open_file, print_rows};
use clap::Parser;
use log::debug;
use std::error::Error;
use std::path::PathBuf;

/// Prints the contents of Parquet file(s)
#[derive(Parser, Debug)]
pub struct CatCommandArgs {
    /// Use CSV format for printing
    #[arg(short, long, conflicts_with = "json")]
    csv: bool,

    /// Use CSV format without a header for printing
    #[arg(long = "no-header", requires = "csv", conflicts_with = "json")]
    csv_no_header: bool,

    /// Use JSON lines format for printing
    #[arg(short, long, conflicts_with = "csv")]
    json: bool,

    #[arg(short, long, conflicts_with = "csv")]
    ndjson: bool,

    /// Parquet files or folders to read from
    locations: Vec<PathBuf>,
}

pub(crate) fn execute(opts: CatCommandArgs) -> Result<(), PQRSError> {
    let format = if opts.json {
        Formats::Json
    } else if opts.csv_no_header {
        Formats::CsvNoHeader
    } else if opts.csv {
        Formats::Csv
    } else if opts.ndjson {
        Formats::NdJson
    } else {
        Formats::Default
    };

    debug!(
        "The locations to read from are: {:?} Using output format: {:?}",
        &opts.locations, format
    );

    for location in &opts.locations {
        match print_rows(open_file(location)?, None, format) {
            Ok(_) => (),
            Err(e) => {
                if e.source().unwrap().to_string().contains("Broken pipe") {
                    // println!("============ PIPE ==========");
                    return Ok(());
                }
                return Err(e)
                // eprintln!("--------------- {:?} ----------", ); 
                // return Ok(());
                // if e.to_string().contains("Broken pipe") {
                //     println!("============ PIPE ==========");
                //     // return Ok(());
                // }
                // return Err(e);
                // eprintln!("zzError while reading file |{:?} \n| {:?}\n |{:?}|", e.cause(), e.description(), e);
                // eprintln!("Error while reading file |{:?}|: |{}|", location);
            }
        }
    }
    Ok(())
}
