extern crate byteorder;
extern crate libflate;

// #[macro_use(bson, doc)]
extern crate bson;
extern crate varinteger;

#[macro_use]
extern crate structopt;
extern crate chrono;
extern crate indicatif;

use std::path::PathBuf;
use structopt::StructOpt;

use std::io;
// use std::io::prelude::*;
use std::io::BufReader;
// use std::io::Reader;
use std::fs::File;
use std::io::Read;
// use byteorder::{LittleEndian, ReadBytesExt};
use indicatif::ProgressBar;

fn decode_file(file_name: &str) -> io::Result<i32> {
    let f = File::open(file_name)?;
    let mut reader = BufReader::new(f);
    // let mut buffer = String::new();

    println!("File {}", file_name);

    let mut v: Vec<u8> = Vec::with_capacity(4 * 1024);
    v.resize(4 * 1024, 0);

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
    return Ok(1);
}

mod ftdc {
    use bson::decode_document;
    use bson::Bson;
    use bson::Document;
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
    use libflate::zlib::{Decoder, Encoder};
    use std::fs::File;
    use std::io::BufRead;
    use std::io::BufReader;
    use std::io::Cursor;
    use std::io::Read;
    use std::str::*;
    use varinteger::decode;
use std::borrow::Borrow;

    pub struct BSONBlockReader {
        reader: BufReader<File>,
    }

    pub enum RawBSONBlock {
        Metadata(Document),
        Metrics(Document),
    }

    impl BSONBlockReader {
        pub fn new(file_name: &str) -> BSONBlockReader {
            let ff = File::open(file_name).unwrap();

            let mut r = BSONBlockReader {
                reader: BufReader::new(ff),
            };

            return r;
        }
    }

    // #[derive(Serialize, Deserialize, Debug)]
    // pub struct MetadataDoc {
    //     #[serde(rename = "_id")]  // Use MongoDB's special primary key field name when serializing
    //     pub id: Date,
    //     pub type: i32,
    //     pub age: i32
    // }

    impl Iterator for BSONBlockReader {
        // add code here
        type Item = RawBSONBlock;

        fn next(&mut self) -> Option<RawBSONBlock> {
            let mut size_buf: [u8; 4] = [0, 0, 0, 0];

            let result = self.reader.read_exact(&mut size_buf);
            if result.is_err() {
                return None;
            }

            // todo: size == 4
            let mut size_rdr = Cursor::new(size_buf);
            let size = size_rdr.read_i32::<LittleEndian>().unwrap();
            println!("size2 {}", size);
            // // Look for the first 4 bytes
            // let mut rdr = Cursor::new(self.buffer);
            // // Note that we use type parameters to indicate which kind of byte order
            // // we want!
            // println!("size {}", size);

            let read_size = size as usize;
            let mut v: Vec<u8> = Vec::with_capacity(read_size);
            v.resize(read_size, 0);

            let result = self.reader.read_exact(&mut v[4..]);
            if result.is_err() {
                return None;
            }

            v.write_i32::<LittleEndian>(size).unwrap();

            println!("size3 {}", size);

            let doc = decode_document(&mut Cursor::new(&v)).unwrap();

            let ftdc_type = doc.get_i32("type").unwrap();

            if ftdc_type == 0 {
                return Some(RawBSONBlock::Metadata(doc));
            }

            return Some(RawBSONBlock::Metrics(doc));
        }
    }



    fn extract_metrics_bson_int(value: &Bson, metrics: &mut Vec<i64>) {
        match value {
            &Bson::FloatingPoint(f) => {
                metrics.push(f as i64);
            }
            &Bson::I64(f) => {
                metrics.push(f);
            }
            &Bson::I32(f) => {
                metrics.push(f as i64);
            }
            &Bson::Boolean(f) => {
                metrics.push(f as i64);
            }
            &Bson::UtcDatetime(f) => {
                metrics.push(f.timestamp() as i64);
            }
            &Bson::TimeStamp(f) => {
                metrics.push(f >> 32 as i64);
                metrics.push(f & 0xffff as i64);
            }
            &Bson::Document(ref o) => {
                extract_metrics_int(o, metrics);
            }
            &Bson::Array(ref a) => {
                for &ref b in a {
                    extract_metrics_bson_int(b, metrics);
                }
            }

            &Bson::JavaScriptCode(_) => {}
            &Bson::JavaScriptCodeWithScope(_, _) => {}
            &Bson::Binary(_, _) => {}
            &Bson::ObjectId(_) => {}

            &Bson::String(_) | &Bson::Null | &Bson::Symbol(_) | &Bson::RegExp(_, _) => {}
        }
    }

    fn extract_metrics_int(doc: &Document, metrics: &mut Vec<i64>) {
        for item in doc {
            let name = item.0;
            let value = item.1;

            extract_metrics_bson_int(value, metrics);
        }
    }

    pub fn extract_metrics(doc: &Document) -> Vec<i64> {
        let mut metrics: Vec<i64> = Vec::new();
        extract_metrics_int(doc, &mut metrics);
        return metrics;
    }

    fn concat2(a1: &str, a2: &str) -> String {
        let mut s = a1.to_string();
        s.push_str(a2);
        return s;
    }

    fn concat3(a1: &str, a2: &str, a3: &str) -> String {
        let mut s = a1.to_string();
        s.push_str(a2);
        s.push('.');
        s.push_str(a3);
        return s;
    }

    fn extract_metrics_paths_bson_int(
        value: &(&String, &Bson),
        prefix: &str,
        metrics: &mut Vec<String>,
    ) {
        let prefix_dot_str = prefix.to_string() + ".";
        let prefix_dot = prefix_dot_str.as_str();
        let ref name = value.0;
        match value.1 {
            &Bson::FloatingPoint(_)
            | &Bson::I64(_)
            | &Bson::I32(_)
            | &Bson::Boolean(_)
            | &Bson::UtcDatetime(_) => {
                let a1 = concat2(prefix_dot, name.as_str());
                metrics.push(a1);
            }
            &Bson::TimeStamp(f) => {
                metrics.push(concat3(prefix_dot, name.as_str(), "t"));
                metrics.push(concat3(prefix_dot, name.as_str(), "i"));
            }
            &Bson::Document(ref o) => {
                extract_metrics_paths_int(o, concat2(prefix_dot, name.as_str()).as_str(), metrics);
            }
            &Bson::Array(ref a) => {
                for &ref b in a {
                    extract_metrics_paths_bson_int(
                        &(&name, b),
                        concat2(prefix_dot, name.as_str()).as_str(),
                        metrics,
                    );
                }
            }

            &Bson::JavaScriptCode(_) => {}
            &Bson::JavaScriptCodeWithScope(_, _) => {}
            &Bson::Binary(_, _) => {}
            &Bson::ObjectId(_) => {}

            &Bson::String(_) | &Bson::Null | &Bson::Symbol(_) | &Bson::RegExp(_, _) => {}
        }
    }

    fn extract_metrics_paths_int(doc: &Document, prefix: &str, metrics: &mut Vec<String>) {
        for item in doc {
            extract_metrics_paths_bson_int(&item, prefix, metrics);
        }
    }

    pub fn extract_metrics_paths(doc: &Document) -> Vec<String> {
        let mut metrics: Vec<String> = Vec::new();
        let s = String::new();
        extract_metrics_paths_int(doc, "", &mut metrics);
        return metrics;
    }

    fn fill_document_bson_int(
        ref_field: (&String, &Bson),
        it: &mut Iterator<Item = &i64>,
        doc: &mut Document,
    ) {
        match ref_field.1 {
            &Bson::FloatingPoint(f) => {
                doc.insert_bson(
                    ref_field.0.to_string(),
                    Bson::FloatingPoint(*it.next().unwrap() as f64),
                );
            }
            &Bson::I64(f) => {
                doc.insert_bson(ref_field.0.to_string(), Bson::I64(*it.next().unwrap()));
            }
            &Bson::I32(f) => {
                doc.insert_bson(
                    ref_field.0.to_string(),
                    Bson::I32(*it.next().unwrap() as i32),
                );
            }
            &Bson::Boolean(f) => {
                doc.insert_bson(
                    ref_field.0.to_string(),
                    Bson::Boolean(*it.next().unwrap() != 0),
                );
            }
            &Bson::UtcDatetime(f) => {
                doc.insert_bson(
                    ref_field.0.to_string(),
                    Bson::UtcDatetime(DateTime::<Utc>::from_utc(
                        NaiveDateTime::from_timestamp(*it.next().unwrap(), 0),
                        Utc,
                    )),
                );
            }
            &Bson::TimeStamp(f) => {
                let p1 = it.next().unwrap();
                let p2 = it.next().unwrap();

                doc.insert_bson(ref_field.0.to_string(), Bson::TimeStamp(p1 << 32 & p2));
            }
            &Bson::Document(ref o) => {
                let mut doc_nested = Document::new();
                for ref_field2 in o {
                    fill_document_bson_int(ref_field2, it, &mut doc_nested);
                }
            }
            &Bson::Array(ref a) => {
                // for &ref b in a {
                //     fill_document_bson_int(value, it, doc);
                // }
            }

            &Bson::JavaScriptCode(ref a) => {
                doc.insert_bson(ref_field.0.to_string(), Bson::JavaScriptCode(a.to_string()));
            }
            &Bson::JavaScriptCodeWithScope(ref a, ref b) => {
                doc.insert_bson(
                    ref_field.0.to_string(),
                    Bson::JavaScriptCodeWithScope(a.to_string(), b.clone()),
                );
            }
            &Bson::Binary(ref a, ref b) => {
                doc.insert_bson(ref_field.0.to_string(), Bson::Binary(*a, b.to_vec()));
            }
            &Bson::ObjectId(ref a) => {
                doc.insert_bson(ref_field.0.to_string(), Bson::ObjectId(a.clone()));
            }
            &Bson::String(ref a) => {
                doc.insert_bson(ref_field.0.to_string(), Bson::String(a.to_string()));
            }
            &Bson::Null => {
                doc.insert_bson(ref_field.0.to_string(), Bson::Null);
            }
            &Bson::Symbol(ref a) => {
                doc.insert_bson(ref_field.0.to_string(), Bson::Symbol(a.to_string()));
            }
            &Bson::RegExp(ref a, ref b) => {
                doc.insert_bson(
                    ref_field.0.to_string(),
                    Bson::RegExp(a.to_string(), b.to_string()),
                );
            }
        }
    }

    pub fn fill_document(ref_doc: &Document, metrics: &[i64]) -> Document {
        let mut doc = Document::new();

        let mut cur = metrics.iter();

        for item in ref_doc {
            fill_document_bson_int(item, &mut cur, &mut doc);
        }

        return doc;
    }


    // pub enum MetricsDocument<'a> {
    //     Reference(&'a Document),
    //     Metrics(Vec<i64>),
    // }

    pub enum MetricsDocument<'a> {
        Reference(&'a Document),
        Metrics(&'a [i64]),
    }

    enum MetricState {
        Reference,
        Metrics,
    }

    pub struct MetricsReader<'a> {
        doc: &'a Document,
        ref_doc: Box<Document>,
        data: Vec<i64>,
        it_state: MetricState,
        sample: i32,
        sample_count: i32,
        raw_metrics: Vec<u64>,
        metrics_count: i32,
    }

    impl<'a> MetricsReader<'a> {
        pub fn new<'b>(doc: &'b Document) -> MetricsReader<'b> {
            return MetricsReader {
                doc,
                ref_doc: Box::default(),
                it_state: MetricState::Reference,
                sample: 0,
                sample_count: 0,
                raw_metrics: Vec::new()
            };
        }
    }

    impl<'a> Iterator for MetricsReader<'a> {
        type Item = &'a MetricsDocument<'a>;

        fn next(&mut self) -> Option<&'a MetricsDocument> {
            if self.raw_metrics.is_empty() {
                let blob = self.doc.get_binary_generic("data").unwrap();

                let mut size_rdr = Cursor::new(&blob);
                let un_size = size_rdr.read_i32::<LittleEndian>().unwrap();
                println!("Uncompressed size {}", un_size);

                // skip the length in the compressed blob
                let mut decoder = Decoder::new(&blob[4..]).unwrap();
                let mut decoded_data = Vec::new();
                decoder.read_to_end(&mut decoded_data).unwrap();

                let mut cur = Cursor::new(&decoded_data);
                self.ref_doc = Box::new(decode_document(&mut cur).unwrap());

                let metric_count = cur.read_i32::<LittleEndian>().unwrap();
                println!("metric_count {}", metric_count);

                self.sample_count = cur.read_i32::<LittleEndian>().unwrap();
                println!("sample_count {}", self.sample_count);

                // Extract metrics from reference document
                //                let ref_metrics = extract_metrics(&self.ref_doc);

                // Decode metrics
                self.raw_metrics.reserve((metric_count * self.sample_count) as usize);

                // TODO: Don't decode all metrics initially
                let mut val: u64 = 0;
                for _ in 0..self.sample_count {
                    for _ in 0..metric_count {
                        let read_size = decode(cur.get_ref(), &mut val);
                        cur.consume(read_size);
                        self.raw_metrics.push(val);
                    }
                }
            }

            match self.it_state {
                MetricState::Reference => {
                    self.it_state = MetricState::Metrics;
                    return Some(&MetricsDocument::Reference(self.ref_doc.borrow()));
                }
                MetricState::Metrics => {
                    if self.sample == self.sample_count {
                        return None;
                    }
                    self.sample += 1;

                    return Some(&MetricsDocument::Metrics(self.raw_metrics[((self.sample - 1) * self.metrics_count)..(self.sample * self.metrics_count)]))
                }
            }
        }
    }
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
#[structopt(name = "ftdc", about = "Full Time Diagnostic Data Capture (FTDC) decoder.")]
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
    let ftdc_metrics = "/data/db/diagnostic.data/metrics.2018-05-08T20-00-04Z-00000";

    decode_file(ftdc_metrics);

    let rdr = ftdc::BSONBlockReader::new(ftdc_metrics);

    for item in rdr {
        match item {
            ftdc::RawBSONBlock::Metadata(doc) => {
                println!("Metadata {}", doc);
            }
            ftdc::RawBSONBlock::Metrics(doc) => {
                let rdr = ftdc::MetricsReader::new(&doc);
                for item in rdr {
                    println!("found metric");
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
