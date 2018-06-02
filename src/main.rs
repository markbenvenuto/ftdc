extern crate byteorder;
extern crate libflate;

// #[macro_use(bson, doc)]
extern crate bson;

use std::io;
// use std::io::prelude::*;
use std::io::BufReader;
// use std::io::Reader;
use std::io::Read;
use std::fs::File;
// use byteorder::{LittleEndian, ReadBytesExt};

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
    use std::io::BufReader;
    use std::fs::File;
    use std::io::Read;
    use std::io::Cursor;
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use bson::Document;
    use bson::Bson;
    use bson::decode_document;
    use libflate::zlib::{Decoder, Encoder};

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

    pub enum MetricsDocument {
        Reference(Document),
        Metrics(Vec<i64>),
    }

    pub struct MetricsReader<'a> {
        doc: &'a Document,
        ref_doc: Box<Document>,
        data: Vec<i64>,
    }


        fn extract_metrics_int(doc: &Document, metrics: &mut Vec<i64>) {
            for item in doc {
                let name = item.0;
                let value = item.1;

                match value {
                    &Bson::FloatingPoint(f)  => { 
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
                            extract_metrics_int(b, metrics);
                        }
                    }

                    &Bson::JavaScriptCode(_) =>{}
                    &Bson::JavaScriptCodeWithScope(_, _) =>{}
                    &Bson::Binary(_,_) =>{}
                    &Bson::ObjectId(_) =>{}

                    &Bson::String(_) | &Bson::Null | &Bson::Symbol(_)
                        | &Bson::RegExp(_, _) 
                        => {}
                }
            } 
        }

        fn extract_metrics(doc: &Document) -> Vec<i64> {
            let mut metrics : Vec<i64> = Vec::new();
            extract_metrics_int(doc, &mut metrics);
            return metrics;
        }

    impl<'a> MetricsReader<'a> {
        pub fn new<'b>(doc: &'b Document) -> MetricsReader<'b> {
            return MetricsReader {
                doc,
                ref_doc: Box::default(),
                data: Vec::new(),
            };
        }


    }

    impl<'a> Iterator for MetricsReader<'a> {
        type Item = MetricsDocument;

        fn next(&mut self) -> Option<MetricsDocument> {
            if self.data.is_empty() {
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

                let sample_count = cur.read_i32::<LittleEndian>().unwrap();
                println!("sample_count {}", sample_count);

                // Extract metrics from reference document

                // Decode metrics
            }

            return None;
        }
    }
}

fn main() {
    println!("Hello, world!");

    let ftdc_metrics = "/data/db/diagnostic.data/metrics.2017-08-23T15-32-45Z-00000";

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
}
