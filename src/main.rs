extern crate byteorder;

#[macro_use(bson, doc)]
extern crate bson;

use std::io;
// use std::io::prelude::*;
use std::io::BufReader;
// use std::io::Reader;
use std::io::Read;
use std::fs::File;
use byteorder::{LittleEndian, ReadBytesExt};


fn decode_file(file_name : &str) -> io::Result<i32>{
    let f = File::open(file_name)?;
    let mut reader = BufReader::new(f);
    // let mut buffer = String::new();

    println!("File {}", file_name );

    let mut v : Vec<u8> = Vec::with_capacity(4 * 1024);
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
use byteorder::{LittleEndian, ReadBytesExt};


    pub struct BSONBlockReader {
        reader: BufReader<File>,
    }

    pub enum RawBSONBlock {
        Metadata(i32),
        Metrics(i32),
    }

    impl BSONBlockReader {
        
        pub fn new(file_name : &str) -> BSONBlockReader {
            
            let ff = File::open(file_name).unwrap();
            
            let mut r = BSONBlockReader {
                reader : BufReader::new(ff),
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
            let mut size_buf:[u8; 4] = [0, 0, 0, 0];            

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
            let mut v : Vec<u8> = Vec::with_capacity(read_size);
            v.resize(read_size, 0);
            
            let result = self.reader.read_exact(&mut v[4..]);
            if result.is_err() {
                return None;
            }
            println!("size3 {}", size);

            // let doc = decode_document(&mut Cursor::new(&self.buffer)).unwrap();

            // shift buffer down
            // self.buffer.po

            return Some(RawBSONBlock::Metadata(123));
        }
    }

}

fn main() {
    println!("Hello, world!");

    let ftdc_metrics = "/data/db/diagnostic.data/metrics.2017-08-23T15-32-45Z-00000";

    decode_file(ftdc_metrics);


    let rdr = ftdc::BSONBlockReader::new(ftdc_metrics);

    for item in rdr {
        println!("found ");
    }

}
