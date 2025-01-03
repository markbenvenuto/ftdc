// Copyright [2024] [Mark Benvenuto]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate ftdc;

use std::collections::BTreeSet;
use std::io::stdout;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;
use std::process::Output;
use std::str::FromStr;

use clap::{Parser, Subcommand, ValueEnum};

use anyhow::Result;
use bson::Document;
use ftdc::extract_metrics;
use ftdc::extract_metrics_paths;
use ftdc::VectorMetricsDocument;
use std::collections::HashMap;

use std::fs::File;

use crate::ftdc::MetricsDocument;

/**
 * TODO:
 * 3. add regex filtering
 * 4. find arg parsing crate
 * 5. Make color thingy and progress report
 *
 */

/// A fictional versioning CLI
#[derive(Debug, Parser)] // requires `derive` feature
#[command(name = "ftdc", author, version)]
#[command(about = "Full Time Diagnostic Data Capture (FTDC) decoder", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Activate debug mode
    #[arg(short = 'd', long = "debug")]
    debug: bool,

    /// Verbose logging
    #[arg(short = 'v', long = "verbose")]
    verbose: bool,
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum OutputFormat {
    Bson,
    Json,
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum FlatOutputFormat {
    CSV,
    Parquet,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Decompress FTDC
    #[command(arg_required_else_help = true)]
    Convert {
        /// Input file
        #[arg(required = true, short, long)]
        input: PathBuf,

        // Output format
        #[arg(
            short,
            long,
            default_value_t = OutputFormat::Json, value_enum
        )]
        format: OutputFormat,

        /// Output file, stdout if not present
        #[arg(required = false, short, long)]
        output: Option<PathBuf>,
    },

    /// Decompress FTDC
    #[command(arg_required_else_help = true)]
    ConvertFlat {
        /// Input file
        #[arg(required = true, short, long)]
        input: PathBuf,

        // Output format
        #[arg(
            short,
            long,
            default_value_t = FlatOutputFormat::CSV, value_enum
        )]
        format: FlatOutputFormat,

        /// Output file, stdout if not present
        #[arg(required = false, short, long)]
        output: Option<PathBuf>,
    },

    // Analyze timings of FTDC capture
    Timings {
        /// Input file
        #[arg(required = true, short, long)]
        input: PathBuf,

        /// Output file, stdout if not present
        #[arg(required = false, short, long)]
        output: Option<PathBuf>,
    },

    // Stats about FTDC files
    Stats {
        /// Input file
        #[arg(required = true, short, long)]
        input: PathBuf,

        /// Output file, stdout if not present
        #[arg(required = false, short, long)]
        output: Option<PathBuf>,
    },

    // Block Stats about Metric Chunks
    BlockStats {
        /// Input file
        #[arg(required = true, short, long)]
        input: PathBuf,

        /// Output file, stdout if not present
        #[arg(required = false, short, long)]
        output: Option<PathBuf>,
    },
}

// fn analyze_doc(doc: &Document, names: &mut HashSet<String>) -> HashMap<String, i64> {

//     let mut v : HashMap<String, i64> = HashMap::<String,i64>::new();
//     // println!("Start : {:?}", doc.get("start"));
//     // println!("End : {:?}", doc.get("end"));
//     let delta = doc.get_datetime("end").unwrap().timestamp_millis() - doc.get_datetime("start").unwrap().timestamp_millis();
//     // println!("delta: {:?}", delta);
//     // print!("{}", serde_json  ::to_string_pretty(doc).unwrap());

//     v.insert("base".to_owned(), delta);

//     for key in doc.keys() {
//         if key == "start" || key == "end" {
//             continue;
//         }

//         let sub = doc.get_document(key).unwrap();
//         let sub_delta = sub.get_datetime("end").unwrap().timestamp_millis() - sub.get_datetime("start").unwrap().timestamp_millis();
//         // println!("sub_delta: {:?}: {:?}", key, sub_delta);
//         names.insert(key.to_owned());
//         v.insert(key.to_owned(), sub_delta);
//     }

//     return v;
// }

fn analyze_ref(doc: &Document, deltas: &mut HashMap<String, Vec<i64>>) -> Result<()> {
    // println!("Start : {:?}", doc.get("start"));
    // println!("End : {:?}", doc.get("end"));
    let delta =
        doc.get_datetime("end")?.timestamp_millis() - doc.get_datetime("start")?.timestamp_millis();
    // println!("delta: {:?}", delta);
    // print!("{}", serde_json  ::to_string_pretty(doc)?);
    if !deltas.contains_key("base") {
        deltas.insert("base".to_owned(), Vec::new());
    }
    deltas.get_mut("base").unwrap().push(delta);
    // v.insert("base".to_owned(), delta);

    for key in doc.keys() {
        if key == "start" || key == "end" {
            continue;
        }

        let sub = doc.get_document(key)?;
        let sub_delta = sub.get_datetime("end")?.timestamp_millis()
            - sub.get_datetime("start")?.timestamp_millis();

        if sub_delta > 10 {
            println!(
                "sub_delta: {}, {}, {}, {}",
                sub.get_datetime("start")?.to_string(),
                sub.get_datetime("end")?.to_string(),
                key,
                sub_delta
            );
        }

        if !deltas.contains_key(key) {
            deltas.insert(key.to_owned(), Vec::new());
        }
        deltas.get_mut(key).unwrap().push(sub_delta);
    }

    Ok(())
}

fn analyze_doc(doc: &Document, deltas: &mut HashMap<String, Vec<i64>>) -> Result<()> {
    // println!("Start : {:?}", doc.get("start"));
    // println!("End : {:?}", doc.get("end"));
    let delta =
        doc.get_datetime("end")?.timestamp_millis() - doc.get_datetime("start")?.timestamp_millis();
    // println!("delta: {:?}", delta);
    // print!("{}", serde_json  ::to_string_pretty(doc)?);

    deltas.get_mut("base").unwrap().push(delta);
    // v.insert("base".to_owned(), delta);

    for key in doc.keys() {
        if key == "start" || key == "end" {
            continue;
        }

        let sub = doc.get_document(key)?;
        let sub_delta = sub.get_datetime("end")?.timestamp_millis()
            - sub.get_datetime("start")?.timestamp_millis();

        // println!("sub_delta2: {:?}: {:?}", key, sub_delta);
        deltas.get_mut(key).unwrap().push(sub_delta);
    }

    Ok(())
}

fn format_doc(format: OutputFormat, doc: &Document, writer: &mut dyn Write) -> Result<()> {
    match format {
        OutputFormat::Bson => {
            let res = bson::to_vec(&doc)?;
            writer.write_all(&res)?;
        }
        OutputFormat::Json => {
            serde_json::to_writer(writer, &doc)?;
        }
    }

    Ok(())
}

fn convert_file(
    rdr: &mut ftdc::BSONBlockReader<File>,
    format: OutputFormat,
    writer: &mut dyn Write,
) -> Result<()> {
    let mut buf_writer = BufWriter::new(writer);

    let mut scratch = Vec::<u8>::new();
    scratch.reserve(1024 * 1024);

    for item in rdr {
        match item {
            ftdc::RawBSONBlock::Metadata(doc) => {
                format_doc(format, &doc, &mut buf_writer)?;
            }
            ftdc::RawBSONBlock::Metrics(doc) => {
                let rdr = ftdc::MetricsReader::new(&doc)?;
                for m_item in rdr.into_iter() {
                    match m_item {
                        MetricsDocument::Reference(d1) => {
                            format_doc(format, &d1, &mut buf_writer)?;
                        }
                        MetricsDocument::Metrics(d1) => {
                            format_doc(format, &d1, &mut buf_writer)?;
                        }
                    };
                }
            }
        }
    }

    Ok(())
}


const SENTINEL_VALUE : usize = 0xfff;

fn write_row(metrics:&Vec<u64>, map_vec:&Vec<usize>, writer: &mut dyn Write) {
    for (idx, &mapping) in map_vec.iter().enumerate() {
        if mapping!= SENTINEL_VALUE {
            write!(writer, "{},", metrics[mapping] );
        } else {
            writer.write(",".as_bytes());
        }
    }

    writer.write("\n".as_bytes());
}

fn convert_flat_file(input: PathBuf, format: FlatOutputFormat, writer: &mut dyn Write) -> Result<()> {
    let mut first_rdr = ftdc::BSONBlockReader::new(input.to_str().unwrap()).unwrap();

    let mut buf_writer = BufWriter::new(writer);

    let mut scratch = Vec::<u8>::new();
    scratch.reserve(1024 * 1024);

    // TODO 
    /*
    1. Map paths to cols
    2. Merge big list
    3. Per section, map col to id
    4. Gen CSV
    5. Make raw metrics block reader
     */
    let mut path_set : BTreeSet<String> = BTreeSet::new();

    // Get the list of columns across ALL blocks
    for item in first_rdr {
        match item {
            ftdc::RawBSONBlock::Metadata(doc) => {
                println!("Skipping metadata blocks")
            }
            ftdc::RawBSONBlock::Metrics(doc) => {
                let rdr = ftdc::VectorMetricsReader::new(&doc)?;
                for m_item in rdr.into_iter() {
                    match m_item {
                        VectorMetricsDocument::Reference(d1) => {
                            let paths = extract_metrics_paths(&d1);
                            let mut ps : BTreeSet<String> = paths.into_iter().map(|x| x.name).collect();
                            path_set.append(&mut ps);
                            break;
                        }
                        VectorMetricsDocument::Metrics(d1) => {
                            panic!("Should not hit this");
                        }
                    };
                }
            }
        }
    }

    // Make a map of name -> column #
    let path_index : HashMap<String, usize> = path_set.into_iter().enumerate().map(|(x, y)| (y,x)).collect();

    println!("Headers: {}", path_index.len());

    let mut header_names : Vec<String> = path_index.iter().map(|x| x.0.clone()).collect();
    header_names.push("ignore_trailer".into());
    let header_names_comma = header_names.join(",");
    // Make header
    buf_writer.write(header_names_comma.as_bytes());
    buf_writer.write("\n".as_bytes());

    let mut second_rdr = ftdc::BSONBlockReader::new(input.to_str().unwrap()).unwrap();

    for item in second_rdr {
        match item {
            ftdc::RawBSONBlock::Metadata(doc) => {
                // ignore
            }
            ftdc::RawBSONBlock::Metrics(doc) => {
                let rdr = ftdc::VectorMetricsReader::new(&doc)?;

                let mut col_list_map : Vec<usize> = vec![SENTINEL_VALUE; path_index.len()];

                for m_item in rdr.into_iter() {
                    match m_item {
                        VectorMetricsDocument::Reference(d1) => {
                            let paths = extract_metrics_paths(&d1);

                            // list of col names
                            let mut block_cols : Vec<String> = paths.into_iter().map(|x| x.name).collect();

                            // block col name -> global col index
                            let block_col_to_global_index : Vec<usize> = block_cols.iter().map( |x| path_index.get(x).expect("Corruption between first and second pass").clone() ).collect();

                            for (local_block_index, &global_block_idx) in block_col_to_global_index.iter().enumerate() {
                                col_list_map[global_block_idx] = local_block_index;
                            }
                            
                            let metrics = extract_metrics(&d1);

    println!("Block Headers: {}", block_cols.len());

                            write_row(&metrics, &col_list_map, &mut buf_writer );
                        }
                        VectorMetricsDocument::Metrics(d1) => {
                            write_row(&d1, &col_list_map, &mut buf_writer );
                        }
                    };
                }
            }
        }
    }

    Ok(())
}


fn main() -> Result<()> {
    let args = Cli::parse();
    // println!("{:?}", args);

    match args.command {
        Commands::Convert {
            input,
            format,
            output,
        } => {
            let mut rdr = ftdc::BSONBlockReader::new(input.to_str().unwrap()).unwrap();

            match output {
                Some(f) => {
                    convert_file(&mut rdr, format, &mut File::open(f)?)?;
                }
                None => {
                    convert_file(&mut rdr, format, &mut stdout().lock())?;
                }
            };
        }

        Commands::Stats { input, output } => {
            let mut total = 0;
            let mut blocks = 0;
            let mut metadata = 0;
            let mut metric_docs = 0;
            let mut reference_docs = 0;

            let rdr = ftdc::BSONBlockReader::new(input.to_str().unwrap()).unwrap();

            for item in rdr {
                blocks += 1;

                match item {
                    ftdc::RawBSONBlock::Metadata(_) => {
                        metadata += 1;
                        total += 1;
                    }
                    ftdc::RawBSONBlock::Metrics(doc) => {
                        let rdr = ftdc::MetricsReader::new(&doc)?;
                        for m_item in rdr.into_iter() {
                            match m_item {
                                MetricsDocument::Reference(_) => {
                                    total += 1;
                                    reference_docs += 1;
                                }
                                MetricsDocument::Metrics(_) => {
                                    total += 1;
                                    metric_docs += 1;
                                }
                            };
                        }
                    }
                }
            }

            println!("Blocks, Metadata, Reference Docs, Metrics Docs, Total");
            println!(
                "{}, {}, {}, {}, {}",
                blocks, metadata, reference_docs, metric_docs, total
            );
        }

        Commands::BlockStats { input, output } => {
            let rdr = ftdc::BSONBlockReader::new(input.to_str().unwrap()).unwrap();

            println!("Type, Chunk Size, Ref Size, Metrics, Samples");

            for item in rdr {
                match item {
                    ftdc::RawBSONBlock::Metadata(doc) => {
                        println!("Metadata, {}, {}, {}, {}", 0, 0, 0, 0);
                    }
                    ftdc::RawBSONBlock::Metrics(doc) => {
                        let rdr = ftdc::MetricsReader::new(&doc)?;

                        println!(
                            "Metrics, {}, {}, {}, {}",
                            rdr.decoded_block.chunk_size_bytes,
                            rdr.decoded_block.ref_doc_size_bytes,
                            rdr.decoded_block.metrics_count,
                            rdr.decoded_block.sample_count
                        );
                    }
                }
            }
        }

        Commands::Timings { input, output } => {
            let mut deltas = HashMap::<String, Vec<i64>>::new();

            let rdr = ftdc::BSONBlockReader::new(input.to_str().unwrap()).unwrap();

            for item in rdr {
                match item {
                    ftdc::RawBSONBlock::Metadata(_) => {}
                    ftdc::RawBSONBlock::Metrics(doc) => {
                        let rdr = ftdc::MetricsReader::new(&doc)?;
                        for m_item in rdr.into_iter() {
                            match m_item {
                                MetricsDocument::Reference(d1) => {
                                    analyze_ref(&d1, &mut deltas)?;
                                }
                                MetricsDocument::Metrics(d1) => {
                                    analyze_doc(&d1, &mut deltas)?;
                                }
                            };
                        }
                    }
                }
            }

            println!("Done");
        }
        Commands::ConvertFlat { input, format, output } => 
        {

            match output {
                Some(f) => {
                    convert_flat_file(input, format, &mut File::create(f)?)?;
                }
                None => {
                    convert_flat_file(input, format, &mut stdout().lock())?;
                }
            };
        }
    }

    Ok(())
}

