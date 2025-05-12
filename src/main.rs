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
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::PathBuf;

use bson::to_document;
use chrono::TimeZone;
use chrono::Utc;
use clap::{Parser, Subcommand, ValueEnum};

use anyhow::Result;
use bson::Document;
use ftdc::extract_metrics;
use ftdc::extract_metrics_paths;
use ftdc::writer::BSONBlockWriter;
use ftdc::MetricsDocument;
use ftdc::VectorMetricsDocument;
use indexmap::IndexMap;
use std::collections::HashMap;

use std::fs::File;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
    // Parquet,
    Prometheus,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Decompress FTDC to JSON
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

    /// Decompress FTDC to CSV, Parquet or Promethesus
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

        /// Sample records in a metric batch
        #[arg(required = false, short, long)]
        sample: Option<u16>,
    },

    /// Analyze timings of FTDC capture
    Timings {
        /// Input file
        #[arg(required = true, short, long)]
        input: PathBuf,
        // /// Output file, stdout if not present
        // #[arg(required = false, short, long)]
        // output: Option<PathBuf>,
    },

    /// Stats about FTDC files
    Stats {
        /// Input file
        #[arg(required = true, short, long)]
        input: PathBuf,
        // /// Output file, stdout if not present
        // #[arg(required = false, short, long)]
        // output: Option<PathBuf>,
    },

    /// Block Stats about Metric Chunks
    BlockStats {
        /// Input file
        #[arg(required = true, short, long)]
        input: PathBuf,
        // /// Output file, stdout if not present
        // #[arg(required = false, short, long)]
        // output: Option<PathBuf>,
    },

    /// Convert Prometheus exposition format file to FTDC
    #[command(arg_required_else_help = true)]
    ConvertProm {
        /// Input file
        #[arg(required = true, short, long)]
        input: PathBuf,

        /// Output file
        #[arg(required = false, short, long)]
        output: PathBuf,
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

const SENTINEL_VALUE: usize = 0xffffffff;

// fn count_commas(input: &str) -> usize {
//     input.chars().filter(|c| *c == ',').count()
// }

trait FlatOutputWriter {
    fn write_header(&mut self, header_names: &Vec<String>) -> Result<()>;
    fn write_row(
        &mut self,
        metrics: &Vec<u64>,
        map_vec: &Vec<usize>,
        start_time: u64,
    ) -> Result<()>;
}

struct CSVWriter<'a> {
    buf_writer: BufWriter<&'a mut dyn Write>,
}

impl<'a> FlatOutputWriter for CSVWriter<'a> {
    fn write_header(&mut self, header_names: &Vec<String>) -> Result<()> {
        let header_names_comma = header_names.join(",");

        // println!("Commas {}", count_commas(&header_names_comma));

        // Make csv header
        self.buf_writer.write(header_names_comma.as_bytes())?;
        self.buf_writer.write("\n".as_bytes())?;

        Ok(())
    }

    fn write_row(
        &mut self,
        metrics: &Vec<u64>,
        map_vec: &Vec<usize>,
        _start_time: u64,
    ) -> Result<()> {
        // let mut s = String::new();
        for (_, &mapping) in map_vec.iter().enumerate() {
            if mapping != SENTINEL_VALUE {
                write!(self.buf_writer, "{},", metrics[mapping])?;
                // s.push_str(&format!("{},", metrics[mapping] ));
            } else {
                self.buf_writer.write("0,".as_bytes())?;
                // s.push_str("0,");
            }
        }

        // println!("Commas2 {}", count_commas(&s));

        self.buf_writer.write("0\n".as_bytes())?;

        Ok(())
    }
}

struct PrometheusWriter<'a> {
    buf_writer: BufWriter<&'a mut dyn Write>,
    header_names: Option<Vec<String>>,
}

impl<'a> FlatOutputWriter for PrometheusWriter<'a> {
    fn write_header(&mut self, header_names_ref: &Vec<String>) -> Result<()> {
        let header_names: Vec<String> = header_names_ref
            .iter()
            .map(|x| x.replace(" ", "_"))
            .collect();

        for header in header_names.iter() {
            self.buf_writer
                .write(format!("# TYPE {} counter\n", header).as_bytes())?;
        }

        self.header_names = Some(header_names);

        Ok(())
    }

    fn write_row(
        &mut self,
        metrics: &Vec<u64>,
        map_vec: &Vec<usize>,
        start_time: u64,
    ) -> Result<()> {
        let header_names = self.header_names.as_ref().unwrap();
        for (header_index, &mapping) in map_vec.iter().enumerate() {
            if mapping != SENTINEL_VALUE {
                write!(
                    self.buf_writer,
                    "{} {} {}\n",
                    header_names[header_index], metrics[mapping], start_time
                )?;
            }
        }
        write!(self.buf_writer, "\n")?;

        Ok(())
    }
}

fn convert_flat_file(
    input: PathBuf,
    format: FlatOutputFormat,
    sample: u16,
    writer: &mut dyn Write,
) -> Result<()> {
    let first_rdr = ftdc::BSONBlockReader::new(input.to_str().unwrap()).unwrap();

    let mut flat_writer: Box<dyn FlatOutputWriter> = match format {
        FlatOutputFormat::CSV => Box::new(CSVWriter {
            buf_writer: BufWriter::new(writer),
        }),
        FlatOutputFormat::Prometheus => Box::new(PrometheusWriter {
            buf_writer: BufWriter::new(writer),
            header_names: None,
        }),
    };

    let mut scratch = Vec::<u8>::new();
    scratch.reserve(1024 * 1024);

    let mut path_set: BTreeSet<String> = BTreeSet::new();

    // Get the list of columns across ALL blocks
    for item in first_rdr {
        match item {
            ftdc::RawBSONBlock::Metadata(_) => {
                println!("Skipping metadata blocks")
            }
            ftdc::RawBSONBlock::Metrics(doc) => {
                let rdr = ftdc::VectorMetricsReader::new(&doc)?;
                for m_item in rdr.into_iter() {
                    match m_item {
                        VectorMetricsDocument::Reference(d1) => {
                            let paths = extract_metrics_paths(&d1);

                            let mut dups: BTreeSet<String> = BTreeSet::new();

                            for p in paths.iter() {
                                if !dups.insert(p.name.clone()) {
                                    println!("Duplicate: {}", p.name);
                                }
                            }
                            // println!("Paths1: {}", paths.len());
                            let mut ps: BTreeSet<String> =
                                paths.into_iter().map(|x| x.name).collect();
                            // println!("Paths1s: {}", ps.len());

                            path_set.append(&mut ps);

                            break;
                        }
                        VectorMetricsDocument::Metrics(_) => {
                            panic!("Should not hit this since we break early");
                        }
                    };
                }
            }
        }
    }

    // Make a map of name -> column #
    let mut header_names: Vec<String> = path_set
        .iter()
        .map(|x| x.clone().replace(",", ""))
        .collect();
    header_names.sort();

    let path_index: HashMap<String, usize> = header_names
        .iter()
        .enumerate()
        .map(|(x, y)| (y.clone(), x))
        .collect();

    // Be lazy so I don't have to track the first or last comma
    header_names.push("ignore_trailer".into());

    // println!("Paths: {}", header_names.len());
    let mut start_index = 0;

    for (idx, hn) in header_names.iter().enumerate() {
        if hn.contains(",") {
            panic!("Header name has a comma which is not escaped {}", hn);
        }

        if hn == ".start" {
            start_index = idx;
        }
    }

    flat_writer.write_header(&header_names)?;

    let second_rdr = ftdc::BSONBlockReader::new(input.to_str().unwrap()).unwrap();

    for item in second_rdr {
        match item {
            ftdc::RawBSONBlock::Metadata(_) => {
                // ignore
            }
            ftdc::RawBSONBlock::Metrics(doc) => {
                let rdr = ftdc::VectorMetricsReader::new(&doc)?;

                let mut col_list_map: Vec<usize> = vec![SENTINEL_VALUE; path_index.len()];
                let mut start_time_idx = 0;

                for (idx, m_item) in rdr.into_iter().enumerate() {
                    match m_item {
                        VectorMetricsDocument::Reference(d1) => {
                            let paths = extract_metrics_paths(&d1);

                            // println!("Paths: {}", paths.len());
                            // list of col names
                            let block_cols: Vec<String> =
                                paths.into_iter().map(|x| x.name.replace(",", "")).collect();

                            // block col name -> global col index
                            let block_col_to_global_index: Vec<usize> = block_cols
                                .iter()
                                .map(|x| {
                                    path_index
                                        .get(x)
                                        .expect("Corruption between first and second pass")
                                        .clone()
                                })
                                .collect();

                            for (local_block_index, &global_block_idx) in
                                block_col_to_global_index.iter().enumerate()
                            {
                                col_list_map[global_block_idx] = local_block_index;

                                if global_block_idx == start_index {
                                    start_time_idx = local_block_index
                                }
                            }

                            let metrics = extract_metrics(&d1);

                            let start_time = metrics[start_time_idx];

                            flat_writer.write_row(&metrics, &col_list_map, start_time)?;
                        }
                        VectorMetricsDocument::Metrics(d1) => {
                            if idx.rem_euclid(sample as usize) != 0 {
                                continue;
                            }

                            let start_time = d1[start_time_idx];

                            flat_writer.write_row(&d1, &col_list_map, start_time)?;
                        }
                    };
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
struct PromRecord {
    label: String,
    value: f64,
    timestamp: i64,
}

fn parse_prom_line(line: &str) -> Option<PromRecord> {
    // Ignore comments
    if line.trim().starts_with('#') {
        return None;
    }

    // Split the line into parts
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() != 3 {
        return None;
    }

    // Parse the label, value, and timestamp
    let label = parts[0].to_string();
    let value: f64 = parts[1].parse().ok()?;
    let timestamp: i64 = parts[2].parse().ok()?;
    Some(PromRecord {
        label,
        value,
        timestamp,
    })
}

fn convert_prom_file(input: PathBuf, output: PathBuf) -> Result<()> {
    let file = File::open(input)?;

    let reader = BufReader::new(file);

    let mut writer = BSONBlockWriter::new_file(&output, 10).unwrap();

    let mut records: Vec<(String, f64)> = Vec::with_capacity(500);
    let mut last_timestamp: i64 = 0;

    for line in reader.lines() {
        let line = line?;
        if let Some(record) = parse_prom_line(&line) {
            if last_timestamp == 0 {
                last_timestamp = record.timestamp;
                continue;
            }

            if last_timestamp != record.timestamp {
                records.sort_by(|a, b| a.0.cmp(&b.0));

                let samples: IndexMap<String, f64> = records.iter().cloned().collect();

                let start = Utc.timestamp_millis_opt(last_timestamp).unwrap();

                let doc = bson::doc![
                    "start" : start,
                    "serverStatus" : to_document(&samples).expect("Expect conversion to bson for metrics never fails"),
                    "end" : start,
                ];

                writer.add_sample(&doc, start)?;

                records.clear();
                last_timestamp = record.timestamp;
            } else {
                records.push((record.label, record.value));
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
                    convert_file(&mut rdr, format, &mut File::create(f)?)?;
                }
                None => {
                    convert_file(&mut rdr, format, &mut stdout().lock())?;
                }
            };
        }
        Commands::Stats { input } => {
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
        Commands::BlockStats { input } => {
            let rdr = ftdc::BSONBlockReader::new(input.to_str().unwrap()).unwrap();

            println!("Type, Chunk Size, Ref Size, Metrics, Samples");

            for item in rdr {
                match item {
                    ftdc::RawBSONBlock::Metadata(_) => {
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
        Commands::Timings { input } => {
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
        Commands::ConvertFlat {
            input,
            format,
            output,
            sample,
        } => {
            match output {
                Some(f) => {
                    convert_flat_file(input, format, sample.unwrap_or(1), &mut File::create(f)?)?;
                }
                None => {
                    convert_flat_file(input, format, sample.unwrap_or(1), &mut stdout().lock())?;
                }
            };
        }
        Commands::ConvertProm { input, output } => {
            convert_prom_file(input, output)?;
        }
    }

    Ok(())
}
