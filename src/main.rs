extern crate byteorder;

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

    struct BSONBlockReader {
        reader: BufReader<File>,
        scratch : Vec<u8>,
        state : State,
        buffer : Vec<u8>
    }

    enum RawBSONBlock {
        Metadata(i32),
        Metrics(i32),
    }

    impl BSONBlockReader {
        
        fn new(file_name : &str) -> BSONBlockReader {
            
            let ff = File::open(file_name).unwrap();
            
            let mut r = BSONBlockReader {
                reader : BufReader::new(ff),
                scratch : Vec::with_capacity(4 * 1024),
                buffer : Vec::with_capacity(4 * 1024),
                state: State::NeedData,
            };

            r.scratch.resize(4 * 1024, 0);

            return r;
        }

        fn read(&mut self) {
            let size = self.reader.read(&mut self.scratch).unwrap();
            self.buffer.append(&mut self.scratch);
        }
    }

    enum State {
        NeedData,
        HaveData,
    }

    impl Iterator for BSONBlockReader {
        // add code here
        type Item = RawBSONBlock;

        fn next(&mut self) -> Option<RawBSONBlock> {
            loop {
            match self.state {
                State::NeedData => {
                    self.read();
                    self.state = State::HaveData;
                    continue;
                }
                State::HaveData => {
                    // Look for the first 4 bytes
                    let size : i32 = 4;

                    //readBSON

                    // shift buffer down
                    self.state = State::NeedData;

                    return None;
                }
            }
            }
        }
    }

}

fn main() {
    println!("Hello, world!");

    let ftdc_metrics = "/data/db/diagnostic.data/metrics.2017-08-23T15-32-45Z-00000";

    decode_file(ftdc_metrics);

}
