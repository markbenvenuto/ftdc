// extern crate byteorder;
// extern crate libflate;

// // #[macro_use(bson, doc)]
// extern crate bson;
// extern crate varinteger;

// #[macro_use]
// extern crate structopt;
// extern crate chrono;
// extern crate indicatif;

extern crate ftdc;

use std::path::PathBuf;
use structopt::StructOpt;

use std::io;
// use std::io::prelude::*;
use std::io::BufReader;
// use std::io::Reader;
use std::fs::File;
use std::io::Read;

use crate::ftdc::MetricsDocument;
// use byteorder::{LittleEndian, ReadBytesExt};
// use indicatif::ProgressBar;

fn decode_file(file_name: &str) -> io::Result<i32> {
    let f = File::open(file_name)?;
    let mut reader = BufReader::new(f);
    // let mut buffer = String::new();

    println!("File {}", file_name);

    let mut v: Vec<u8> = vec!(0; 4 * 1024);

    loop {
        // read a line into buffer
        //reader.read_line(&mut buffer)?;

        let size = reader.read(&mut v).unwrap();

        println!("Read {} ", size);

        if size == 0 {
            break;
        }
    }

    println!("Done Reading");
    Ok(1)
}

/**
 * TODO:
 * 1. add to bson
 * 2. add to json
 * 3. add regex filtering
 * 4. find arg parsing crate
 * 5. Make color thingy and progress report
 *
 */

#[derive(Debug, StructOpt)]
#[structopt(
    name = "ftdc",
    about = "Full Time Diagnostic Data Capture (FTDC) decoder."
)]
struct Opt {
    /// Activate debug mode
    #[structopt(short = "d", long = "debug")]
    debug: bool,
    /// Set speed
    #[structopt(short = "v", long = "verbose")]
    verbose: bool,
    /// Input file
    #[structopt(parse(from_os_str))]
    input: Option<PathBuf>,
    /// Output file, stdout if not present
    #[structopt(parse(from_os_str))]
    output: Option<PathBuf>,
}

fn main() {
    println!("Hello, world!");

    let opt = Opt::from_args();
    println!("{:?}", opt);

    // let ftdc_metrics = "/data/db/diagnostic.data/metrics.2018-03-15T02-18-51Z-00000";
    // let ftdc_metrics = "/Users/mark/mongo/data/diagnostic.data/metrics.2022-08-11T19-59-54Z-00000";
    let ftdc_metrics = "/Users/mark/projects/ftdc/metrics.2022-05-12T08-52-03Z-00000";

    decode_file(ftdc_metrics);

    let rdr = ftdc::BSONBlockReader::new(ftdc_metrics);

    for item in rdr {
        match item {
            ftdc::RawBSONBlock::Metadata(doc) => {
                println!("Metadata {}", doc);
            }
            ftdc::RawBSONBlock::Metrics(doc) => {
                let mut rdr = ftdc::MetricsReader::new(&doc);

                for item in rdr.into_iter() {
                    println!("found metric");
                    println!("metric {:?}", item);

                    match item {
                        MetricsDocument::Reference(_) => {}
                        MetricsDocument::Metrics(_) => {
                            return;
                        }
                    }

                }
            }
        }
    }

    /*
        let bar = ProgressBar::new(1000);
    for _ in 0..1000 {
        bar.inc(1);
        // ...
    }
    bar.finish();
        */
}
