use std::fs::File;
use std::io::BufReader;
use std::io::Read;

use anyhow::Result;
use bson::Document;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use libflate::zlib::Decoder;
use std::io::Cursor;
use std::rc::Rc;

use crate::util::extract_metrics;
use crate::util::extract_metrics_paths;
use crate::util::fill_document;

#[derive(Debug)]
pub enum MetricsDocument {
    Reference(Rc<Document>),
    Metrics(Document),
}

enum MetricState {
    Reference,
    Metrics,
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

// TODO - use lifetime to avoid copy of vec
#[derive(Debug)]
pub enum VectorMetricsDocument {
    Reference(Rc<Document>),
    Metrics(Vec<u64>),
}

pub struct DecodedMetricBlock {
    pub ref_doc: Rc<Document>,
    pub ref_doc_size_bytes: usize,

    pub chunk_size_bytes: usize,
    pub sample_count: i32,
    pub metrics_count: i32,

    pub(crate) raw_metrics: Vec<u64>,
}

pub fn decode_metric_block<'a>(doc: &'a Document) -> Result<DecodedMetricBlock> {
    let blob = doc.get_binary_generic("data")?;
    let chunk_size_bytes = blob.len();

    let mut size_rdr = Cursor::new(&blob);
    let _un_size = size_rdr.read_i32::<LittleEndian>()?;
    // println!("Uncompressed size {}", un_size);

    // skip the length in the compressed blob
    let mut decoded_data = Vec::<u8>::new();
    let mut decoder = Decoder::new(&blob[4..])?;
    decoder.read_to_end(&mut decoded_data)?;

    let mut cur = Cursor::new(&decoded_data);

    let ref_doc_size_bytes = cur.read_i32::<LittleEndian>()? as usize;
    cur.set_position(0);

    let ref_doc = Rc::new(bson::from_reader(&mut cur)?);

    // let mut pos1: usize = cur.position() as usize;
    // println!("pos:{:?}", pos1);

    let metrics_count = cur.read_i32::<LittleEndian>()?;
    // println!("metric_count {}", metrics_count);

    let sample_count = cur.read_i32::<LittleEndian>()?;
    // println!("sample_count {}", sample_count);

    // Extract metrics from reference document
    let ref_metrics = extract_metrics(&ref_doc);
    if ref_metrics.len() != metrics_count as usize {
        let paths = extract_metrics_paths(&ref_doc);
        for p in paths {
            println!("{}", p.name);
        }
    }
    assert_eq!(ref_metrics.len(), metrics_count as usize);
    // println!("{:?}", ref_metrics);

    // println!("Ref: Sample {} Metric {}", self.sample_count, self.metrics_count);

    // Decode metrics
    let mut raw_metrics = Vec::<u64>::with_capacity((metrics_count * sample_count) as usize);

    let mut zeros_count = 0;

    let mut pos: usize = cur.position() as usize;
    // println!("pos:{:?}", pos);
    let buf = decoded_data.as_ref();

    if sample_count == 0 || metrics_count == 0 {
        return Ok(DecodedMetricBlock {
            ref_doc,
            ref_doc_size_bytes,
            chunk_size_bytes,
            sample_count,
            metrics_count,
            raw_metrics,
        });
    }

    raw_metrics.resize((sample_count * metrics_count) as usize, 0);

    for i in 0..metrics_count {
        for j in 0..sample_count {
            // eprintln!("r{},{}", i, j);
            if zeros_count > 0 {
                raw_metrics[get_array_offset(sample_count, j, i)] = 0;
                zeros_count -= 1;
                continue;
            }

            let mut val: u64 = 0;
            let read_size = varinteger::decode_with_offset(buf, pos, &mut val);
            pos += read_size;

            if val == 0 {
                // Read zeros count
                let read_size = varinteger::decode_with_offset(buf, pos, &mut zeros_count);
                pos += read_size;
            }

            raw_metrics[get_array_offset(sample_count, j, i)] = val;
        }
    }

    assert_eq!(pos, buf.len());

    // eprintln!("ddd: {:?}", raw_metrics);

    // Inflate the metrics
    for i in 0..metrics_count {
        let (v, _) = raw_metrics[get_array_offset(sample_count, 0, i)]
            .overflowing_add(ref_metrics[i as usize] as u64);
        raw_metrics[get_array_offset(sample_count, 0, i)] = v;
    }

    for i in 0..metrics_count {
        for j in 1..sample_count {
            let (v, _) = raw_metrics[get_array_offset(sample_count, j, i)]
                .overflowing_add(raw_metrics[get_array_offset(sample_count, j - 1, i)]);
            raw_metrics[get_array_offset(sample_count, j, i)] = v;
        }
    }

    Ok(DecodedMetricBlock {
        ref_doc,
        ref_doc_size_bytes,
        chunk_size_bytes,
        sample_count,
        metrics_count,
        raw_metrics,
    })
}

// TODO - make this a wrapper around VectorMetricsReader
pub struct MetricsReader<'a> {
    _doc: &'a Document,
    pub decoded_block: DecodedMetricBlock,

    it_state: MetricState,
    sample: i32,
    scratch: Vec<u64>,
}

impl<'a> MetricsReader<'a> {
    pub fn new<'b>(doc: &'b Document) -> Result<MetricsReader<'b>> {
        let db = decode_metric_block(doc)?;
        let s = vec![0; db.metrics_count as usize];

        Ok(MetricsReader {
            _doc: doc,
            decoded_block: db,
            it_state: MetricState::Reference,
            sample: 0,
            scratch: s,
        })
    }
}

/**
 * Compute the offset into an array for given (sample, metric) pair
 */
fn get_array_offset(sample_count: i32, sample: i32, metric: i32) -> usize {
    ((metric * sample_count) + sample) as usize
}

impl<'a> Iterator for MetricsReader<'a> {
    type Item = MetricsDocument;

    fn next(&mut self) -> Option<MetricsDocument> {
        match self.it_state {
            MetricState::Reference => {
                self.it_state = MetricState::Metrics;

                Some(MetricsDocument::Reference(
                    self.decoded_block.ref_doc.clone(),
                ))
            }
            MetricState::Metrics => {
                if self.sample == self.decoded_block.sample_count {
                    return None;
                }

                self.sample += 1;

                for i in 0..self.decoded_block.metrics_count {
                    self.scratch[i as usize] = self.decoded_block.raw_metrics
                        [get_array_offset(self.decoded_block.sample_count, self.sample - 1, i)];
                }

                let d = fill_document(&self.decoded_block.ref_doc, &self.scratch);
                Some(MetricsDocument::Metrics(d))
            }
        }
    }
}

pub struct VectorMetricsReader<'a> {
    _doc: &'a Document,
    pub decoded_block: DecodedMetricBlock,

    it_state: MetricState,
    sample: i32,
    scratch: Vec<u64>,
}

impl<'a> VectorMetricsReader<'a> {
    pub fn new<'b>(doc: &'b Document) -> Result<VectorMetricsReader<'b>> {
        let db = decode_metric_block(doc)?;
        let s = vec![0; db.metrics_count as usize];

        Ok(VectorMetricsReader {
            _doc: doc,
            decoded_block: db,
            it_state: MetricState::Reference,
            sample: 0,
            scratch: s,
        })
    }

    pub fn get_metrics_count(&self) -> usize {
        self.scratch.len()
    }
}

impl<'a> Iterator for VectorMetricsReader<'a> {
    type Item = VectorMetricsDocument;

    fn next(&mut self) -> Option<VectorMetricsDocument> {
        match self.it_state {
            MetricState::Reference => {
                self.it_state = MetricState::Metrics;

                Some(VectorMetricsDocument::Reference(
                    self.decoded_block.ref_doc.clone(),
                ))
            }
            MetricState::Metrics => {
                if self.sample == self.decoded_block.sample_count {
                    return None;
                }

                self.sample += 1;

                for i in 0..self.decoded_block.metrics_count {
                    self.scratch[i as usize] = self.decoded_block.raw_metrics
                        [get_array_offset(self.decoded_block.sample_count, self.sample - 1, i)];
                }

                Some(VectorMetricsDocument::Metrics(self.scratch.clone()))
            }
        }
    }
}
