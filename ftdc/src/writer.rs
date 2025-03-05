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

use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;

use anyhow::Result;
use bson::Bson;
use bson::Document;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use libflate::zlib::Encoder;
use std::io::Cursor;

use crate::util::gen_metadata_document;
use crate::util::gen_metrics_document;

pub struct BSONMetricsCompressor {
    // samples: usize,
    max_samples: usize,

    metrics: usize,

    // TODO - change this to deltas array so we keep this column oriented in memory
    // also store the un delta encoded metrics as we accumulate samples
    metric_vec: Vec<Vec<u64>>,
    ref_doc: Document,
    ref_doc_vec: Vec<u64>,
    ref_date: DateTime<Utc>, // blocks : Vec<Vec<u8>>,
}

#[derive(Debug, PartialEq)]
pub enum AddResult {
    NewBlock(Option<(Vec<u8>, DateTime<Utc>)>),
    ExistingBlock,
}

// TODO - rename this as metric block compressor
// Create FTDC file writer
impl BSONMetricsCompressor {
    pub fn new(max_samples: usize) -> BSONMetricsCompressor {
        BSONMetricsCompressor {
            // samples : 0,
            max_samples,
            metrics: 0,
            metric_vec: Vec::new(),
            ref_doc: Document::new(),
            ref_doc_vec: Vec::new(),
            ref_date: Utc.timestamp_nanos(0),
        }
    }

    // TODO - report if new block was started
    pub fn add_doc(&mut self, doc: &Document, date: DateTime<Utc>) -> Result<AddResult> {
        let mut met_vec = Vec::new();
        extract_metrics_int(doc, &mut met_vec);

        // first document
        if self.ref_doc.is_empty() {
            self.ref_doc = doc.clone();
            self.ref_date = date;

            self.metric_vec.clear();
            self.metrics = met_vec.len();

            self.ref_doc_vec = met_vec;
            // self.samples = 0;

            // self.metric_vec.push(met_vec);
            return Ok(AddResult::NewBlock(None));
        }

        // If metric count the same?
        if self.metrics == met_vec.len() && self.metric_vec.len() < self.max_samples - 1 {
            self.metric_vec.push(met_vec);

            Ok(AddResult::ExistingBlock)
        } else {
            // New block, flush chunk
            let block = self.flush_block()?;

            self.ref_doc = doc.clone();
            self.ref_date = date;

            self.metric_vec.clear();
            self.metrics = met_vec.len();

            self.ref_doc_vec = met_vec;

            Ok(AddResult::NewBlock(Some((block, date))))
        }
    }

    fn flush(&mut self) -> Result<Option<(Vec<u8>, DateTime<Utc>)>> {
        if self.metric_vec.is_empty() {
            return Ok(None);
        }

        Ok(Some((self.flush_block()?, self.ref_date)))
    }

    // Compress Metric Vectors
    fn compress_metric_vec(&mut self) -> Vec<u8> {
        if self.metric_vec.is_empty() {
            return Vec::new();
        }
        // assert!(!self.metric_vec.is_empty());

        // eprintln!("rdv: {:?}", self.ref_doc_vec);
        // eprintln!("ccc: {:?}", self.metric_vec);

        // Do delta calculations
        let metric_count = self.ref_doc_vec.len();
        let sample_count = self.metric_vec.len();

        for s in (1..sample_count).rev() {
            for m in 0..metric_count {
                self.metric_vec[s][m] =
                    self.metric_vec[s][m].wrapping_sub(self.metric_vec[s - 1][m]);
            }
        }
        // eprintln!("ccc: {:?}", self.metric_vec);

        for m in 0..metric_count {
            self.metric_vec[0][m] = self.metric_vec[0][m].wrapping_sub(self.ref_doc_vec[m]);
        }

        // eprintln!("ccc: {:?}", self.metric_vec);

        // Do RLE
        let mut count_zeros = 0;
        let mut out = vec![0; metric_count * sample_count * 8];

        let mut offset = 0;

        for m in 0..metric_count {
            for s in 0..sample_count {
                let v = self.metric_vec[s][m];

                if v != 0 {
                    if count_zeros > 0 {
                        let write_size = varinteger::encode_with_offset(0, &mut out, offset);
                        offset += write_size;

                        let write_size =
                            varinteger::encode_with_offset(count_zeros - 1, &mut out, offset);
                        offset += write_size;

                        count_zeros = 0;
                    }

                    let write_size = varinteger::encode_with_offset(v, &mut out, offset);
                    offset += write_size;
                } else {
                    count_zeros += 1;
                }
            }
        }

        if count_zeros > 0 {
            // eprintln!("cz: {:?}", count_zeros);

            let write_size = varinteger::encode_with_offset(0, &mut out, offset);
            offset += write_size;

            let write_size = varinteger::encode_with_offset(count_zeros - 1, &mut out, offset);
            offset += write_size;
        }

        out.resize(offset, 0);

        out
    }

    ///
    /// Format
    /// i32 littlendian
    /// zlib_block
    ///
    /// zlib_block
    /// bson doc
    /// i32 metric
    /// i32 sample
    /// bytes block
    pub fn flush_block(&mut self) -> Result<Vec<u8>> {
        let ref_vec = bson::to_vec(&self.ref_doc)?;

        let mut uncompressed_block: Vec<u8> =
            Vec::with_capacity(4 + ref_vec.len() + self.metrics * self.metric_vec.len());

        uncompressed_block.write_all(&ref_vec)?;

        uncompressed_block.write_i32::<LittleEndian>(self.metrics as i32)?;
        uncompressed_block.write_i32::<LittleEndian>(self.metric_vec.len() as i32)?;

        // Delta & RLE encode
        let encoded_block = self.compress_metric_vec();
        uncompressed_block.write_all(&encoded_block)?;

        // Compress
        let mut encoder = Encoder::new(Vec::new())?;
        encoder.write_all(&uncompressed_block)?;
        let encoded_data = encoder.finish().into_result()?;

        // Make final block
        let mut final_block = Vec::<u8>::with_capacity(4 + encoded_data.len());
        final_block.write_i32::<LittleEndian>(uncompressed_block.len() as i32)?;
        final_block.write_all(&encoded_data)?;

        Ok(final_block)
    }
}

pub struct BSONBlockWriter<W: Write> {
    writer: BufWriter<W>,
    compressor: BSONMetricsCompressor,
}

impl BSONBlockWriter<File> {
    pub fn new_file(file_name: &PathBuf, max_samples: usize) -> Result<BSONBlockWriter<File>> {
        let ff = File::create(file_name)?;

        Ok(BSONBlockWriter {
            writer: BufWriter::new(ff),
            compressor: BSONMetricsCompressor::new(max_samples),
        })
    }
}

impl BSONBlockWriter<bytes::buf::Writer<Vec<u8>>> {
    pub fn new_bytes(
        buf_mut: &mut bytes::buf::Writer<Vec<u8>>,
        max_samples: usize,
    ) -> Result<BSONBlockWriter<&mut bytes::buf::Writer<Vec<u8>>>> {
        Ok(BSONBlockWriter {
            writer: BufWriter::new(buf_mut),
            compressor: BSONMetricsCompressor::new(max_samples),
        })
    }
}

fn write_doc_to_writer(writer: &mut dyn Write, doc: &Document) -> Result<()> {
    let mut buf = Vec::new();
    doc.to_writer(&mut buf)?;

    writer.write_all(&buf)?;
    Ok(())
}

// TODO - reduce copies by using raw bson api?
impl<W: Write> BSONBlockWriter<W> {
    pub fn add_metdata_doc(&mut self, doc: &Document, date: DateTime<Utc>) -> Result<()> {
        let md_doc = gen_metadata_document(doc, date);

        write_doc_to_writer(&mut self.writer, &md_doc)
    }

    pub fn add_sample(&mut self, doc: &Document, sample_date: DateTime<Utc>) -> Result<()> {
        let result = self.compressor.add_doc(doc, sample_date)?;

        match result {
            AddResult::ExistingBlock => {
                // Do Nothing
            }
            AddResult::NewBlock(block_opt) => {
                if let Some((block, date)) = block_opt {
                    let metric_doc = gen_metrics_document(&block, date);

                    write_doc_to_writer(&mut self.writer, &metric_doc)?
                }
            }
        }

        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if let Some((block, date)) = self.compressor.flush()? {
            let metric_doc = gen_metrics_document(&block, date);

            write_doc_to_writer(&mut self.writer, &metric_doc)?
        }

        Ok(())
    }
}

pub struct BSONBlockReader<R: Read> {
    reader: BufReader<R>,
}

pub enum RawBSONBlock {
    Metadata(Document),
    Metrics(Document),
}

impl BSONBlockReader<File> {
    pub fn new(file_name: &str) -> Result<BSONBlockReader<File>> {
        let ff = File::open(file_name)?;

        Ok(BSONBlockReader {
            reader: BufReader::new(ff),
        })
    }
}

impl<R: Read> BSONBlockReader<R> {
    pub fn new_reader(reader: R) -> Result<BSONBlockReader<R>> {
        Ok(BSONBlockReader {
            reader: BufReader::<R>::new(reader),
        })
    }
}

// #[derive(Serialize, Deserialize, Debug)]
// pub struct MetadataDoc {
//     #[serde(rename = "_id")]  // Use MongoDB's special primary key field name when serializing
//     pub id: Date,
//     pub type: i32,
//     pub age: i32
// }

impl<R: Read> Iterator for BSONBlockReader<R> {
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
        // println!("size2 {}", size);
        // // Look for the first 4 bytes
        // let mut rdr = Cursor::new(self.buffer);
        // // Note that we use type parameters to indicate which kind of byte order
        // // we want!
        // println!("size {}", size);

        let read_size = size as usize;
        let mut v: Vec<u8> = Vec::with_capacity(read_size);

        v.write_i32::<LittleEndian>(size).unwrap();

        v.resize(read_size, 0);
        let result = self.reader.read_exact(&mut v[4..]);
        if result.is_err() {
            return None;
        }

        // println!("size3 {}", v.len());
        // println!("Pos: {}\n", self.reader.stream_position().unwrap());

        let doc: Document = bson::from_reader(&mut Cursor::new(&v)).unwrap();

        let ftdc_type = doc.get_i32("type").unwrap();

        if ftdc_type == 0 {
            return Some(RawBSONBlock::Metadata(doc));
        } else if ftdc_type == 2 {
            // TODO - fix
            return Some(RawBSONBlock::Metadata(doc));
        }

        Some(RawBSONBlock::Metrics(doc))
    }
}

fn extract_metrics_bson_int(value: &Bson, metrics: &mut Vec<u64>) {
    match value {
        &Bson::Double(f) => {
            metrics.push(f as u64);
        }
        &Bson::Int64(f) => {
            metrics.push(f as u64);
        }
        &Bson::Int32(f) => {
            metrics.push(f as u64);
        }
        &Bson::Decimal128(_) => {
            panic!("Decimal128 not implemented")
        }
        &Bson::Boolean(f) => {
            metrics.push(f as u64);
        }
        &Bson::DateTime(f) => {
            metrics.push(f.timestamp_millis() as u64);
        }
        &Bson::Timestamp(f) => {
            metrics.push(f.time as u64);
            metrics.push(f.increment as u64);
        }
        Bson::Document(o) => {
            extract_metrics_int(o, metrics);
        }
        Bson::Array(a) => {
            for b in a {
                extract_metrics_bson_int(b, metrics);
            }
        }

        &Bson::JavaScriptCode(_) => {}
        &Bson::JavaScriptCodeWithScope(_) => {}
        &Bson::Binary(_) => {}
        &Bson::ObjectId(_) => {}
        &Bson::DbPointer(_) => {}
        &Bson::MaxKey | &Bson::MinKey | &Bson::Undefined => {}
        &Bson::String(_) | &Bson::Null | &Bson::Symbol(_) | &Bson::RegularExpression(_) => {}
    }
}

fn extract_metrics_int(doc: &Document, metrics: &mut Vec<u64>) {
    for item in doc {
        let value = item.1;
        // eprintln!("ee:{:?}",item);
        extract_metrics_bson_int(value, metrics);
    }
}
