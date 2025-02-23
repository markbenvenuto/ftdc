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

pub mod reader;
pub mod util;
pub mod writer;

pub use reader::BSONBlockReader;
pub use reader::MetricsDocument;
pub use reader::MetricsReader;
pub use reader::RawBSONBlock;
pub use reader::VectorMetricsDocument;
pub use reader::VectorMetricsReader;
pub use util::extract_metrics;
pub use util::extract_metrics_paths;

// pub enum MetricsDocument<'a> {
//     Reference(&'a Document),
//     Metrics(Vec<i64>),
// }

// pub enum MetricsDocument<'a> {
//     Reference(&'a Box<Document>),
//     Metrics(Document),
//     // Metrics(&'a [i64]),
// }

// struct CompressorTee {
//     docs: Vec<Document>,

// }

// impl CompressorTee {
//     fn new() -> CompressorTee {
//         CompressorTee {
//             docs : vec![]
//         }
//     }

//     fn add_doc(&mut self, doc: &Document) {

//     }

//     fn validate(&mut self) {

//     }
// }

// extern crate assert_ok;
#[cfg(test)]
mod test {
    use super::reader::decode_metric_block;
    use super::writer::{AddResult, BSONBlockWriter, BSONMetricsCompressor};
    use assert_ok::assert_ok;
    use bson::doc;
    use bson::spec::BinarySubtype;
    use bytes::BufMut;

    #[test]
    fn test_roundtrip_compressor() {
        let mut writer = BSONMetricsCompressor::new(3);

        assert_eq!(
            writer.add_doc(&doc! {"a": 1, "x" : 2, "s" : "t"}).unwrap(),
            AddResult::NewBlock(None)
        );
        assert_eq!(
            writer.add_doc(&doc! {"a": 2, "x" : 2, "s" : "t"}).unwrap(),
            AddResult::ExistingBlock
        );
        assert_eq!(
            writer.add_doc(&doc! {"a": 3, "x" : 2, "s" : "t"}).unwrap(),
            AddResult::ExistingBlock
        );

        let addresult = writer.add_doc(&doc! {"a": 7, "x" : 9, "s" : "t"}).unwrap();

        assert_ne!(addresult, AddResult::ExistingBlock);

        match addresult {
            AddResult::ExistingBlock => {
                assert!(false);
            }
            AddResult::NewBlock(met_opt) => {
                let met = met_opt.unwrap();

                /*
                let mut rdr = BSONBlockReader::new_reader(Cursor::new(met)).unwrap();

                let bb = rdr.next();
                assert!(bb.is_some());
                */

                let d1 =
                    doc! { "data" : bson::Binary{subtype: BinarySubtype::Generic, bytes: met} };
                let dmbr = decode_metric_block(&d1);
                assert!(dmbr.is_ok());
                let dmb = dmbr.unwrap();
                assert_eq!(dmb.sample_count, 2);
                assert_eq!(dmb.metrics_count, 2);
                eprintln!("{:?}", dmb.ref_doc);
                assert_eq!(dmb.raw_metrics, vec![2, 3, 2, 2]);
            }
        }
    }

    #[test]
    fn test_roundtrip_bson() {
        let mut buf = Vec::with_capacity(1024).writer();

        let mut writer = BSONBlockWriter::new_bytes(&mut buf, 3).unwrap();

        assert_ok!(writer.add_sample(&doc! {"a": 1, "x" : 2, "s" : "t"}));
        assert_ok!(writer.add_sample(&doc! {"a": 2, "x" : 2, "s" : "t"}));
        assert_ok!(writer.add_sample(&doc! {"a": 3, "x" : 2, "s" : "t"}));

        //et addresult = writer.add_doc(&doc! {"a": 7, "x" : 9, "s" : "t"}).unwrap();
    }

    // TODO - test duplicate fields - will need RAW BSON API
    /*
    > .systemMetrics.mounts./boot/efi.capacity
    > .systemMetrics.mounts./boot/efi.available
    > .systemMetrics.mounts./boot/efi.free
     */
}
