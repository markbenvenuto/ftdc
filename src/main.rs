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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use bson::Document;
use structopt::StructOpt;

use std::io;
// use std::io::prelude::*;
use std::io::BufReader;
// use std::io::Reader;
use std::fs::File;
use std::io::Read;

use crate::ftdc::MetricsDocument;

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

fn analyze_ref(doc: &Document, deltas: &mut HashMap<String, Vec<i64> >)  {

    // println!("Start : {:?}", doc.get("start"));
    // println!("End : {:?}", doc.get("end"));
    let delta = doc.get_datetime("end").unwrap().timestamp_millis() - doc.get_datetime("start").unwrap().timestamp_millis();
    // println!("delta: {:?}", delta);
    // print!("{}", serde_json  ::to_string_pretty(doc).unwrap());
    if !deltas.contains_key("base") {
        deltas.insert("base".to_owned(), Vec::new());
    }
    deltas.get_mut("base").unwrap().push(delta);
    // v.insert("base".to_owned(), delta);


    for key in doc.keys() {
        if key == "start" || key == "end" {
            continue;
        }

        let sub = doc.get_document(key).unwrap();
        let sub_delta = sub.get_datetime("end").unwrap().timestamp_millis() - sub.get_datetime("start").unwrap().timestamp_millis();

        if(sub_delta > 10) {
            println!("sub_delta: {}, {}, {}, {}", sub.get_datetime("start").unwrap().to_string(), sub.get_datetime("end").unwrap().to_string(), key, sub_delta);
        }

        if !deltas.contains_key(key) {
            deltas.insert(key.to_owned(), Vec::new());
        }
        deltas.get_mut(key).unwrap().push(sub_delta);
    }
}


fn analyze_doc(doc: &Document, deltas: &mut HashMap<String, Vec<i64> >)  {

    // println!("Start : {:?}", doc.get("start"));
    // println!("End : {:?}", doc.get("end"));
    let delta = doc.get_datetime("end").unwrap().timestamp_millis() - doc.get_datetime("start").unwrap().timestamp_millis();
    // println!("delta: {:?}", delta);
    // print!("{}", serde_json  ::to_string_pretty(doc).unwrap());

    deltas.get_mut("base").unwrap().push(delta);
    // v.insert("base".to_owned(), delta);


    for key in doc.keys() {
        if key == "start" || key == "end" {
            continue;
        }

        let sub = doc.get_document(key).unwrap();
        let sub_delta = sub.get_datetime("end").unwrap().timestamp_millis() - sub.get_datetime("start").unwrap().timestamp_millis();

        // println!("sub_delta2: {:?}: {:?}", key, sub_delta);
        deltas.get_mut(key).unwrap().push(sub_delta);
    }
}


fn main() {
    // println!("Hello, world!");

    let opt = Opt::from_args();
    // println!("{:?}", opt);

    // let ftdc_metrics = "/data/db/diagnostic.data/metrics.2018-03-15T02-18-51Z-00000";
    // let ftdc_metrics = "/Users/mark/mongo/data/diagnostic.data/metrics.2022-08-11T19-59-54Z-00000";
    // let ftdc_metrics = "/Users/mark/projects/ftdc/metrics.2022-05-12T08-52-03Z-00000";
    // let ftdc_metrics = "/Users/mark/projects/ftdc/metrics.2024-01-23T17-15-41Z-00000";
    // let ftdc_metrics = "/Users/mark/projects/ftdc/metrics.2024-01-23T00-01-07Z-00000";
    // let ftdc_metrics = "/home/mark/projects/ftdc/aa/WorkloadOutput/reports-2024-01-24T06:33:28.438224+00:00/majority_reads10_k_threads/mongod.0/diagnostic.data/metrics.2024-01-24T06-27-14Z-00000";

    let ftdc_metrics = "/home/mark/projects/ftdc/ac/WorkloadOutput/reports-2024-01-24T06:27:26.457730+00:00/majority_writes10_k_threads/mongod.0/diagnostic.data/metrics.2024-01-24T06-19-50Z-00000";

    let mut names = HashSet::<String>::new();

    names.insert("base".to_owned());

    let rdr = ftdc::BSONBlockReader::new(ftdc_metrics);

    // use builders
// let mut builder = PrimitiveChunkedBuilder::<UInt32Type>::new("foo", 10);
// for value in 0..10 {
//     builder.append_value(value);
// }
// let ca = builder.finish();

    let mut deltas = HashMap::<String, Vec<i64> >::new();

    let mut i = 0;
    // println!("[");
    let mut c = 0;
    let mut b = 0;
    let mut m = 0;
    let mut r = 0;


    for item in rdr {
         b += 1;

        match item {
            ftdc::RawBSONBlock::Metadata(_) => {
            }
            ftdc::RawBSONBlock::Metrics(doc) => {
                let mut rdr = ftdc::MetricsReader::new(&doc);
                for m_item in rdr.into_iter() {

                    match m_item {
                        MetricsDocument::Reference(d1) => {
                            analyze_ref(&d1, &mut deltas);
                            c += 1;
                            r += 1;
                        }
                        MetricsDocument::Metrics(d1) => {
                            analyze_doc(&d1, &mut deltas);
                            m += 1;
                            c += 1;

                        }
                    };


                    if( c % 100 == 0) {
                        println!("{}, {}, {}, {}", b ,c, r, m);
                    }
                }
            }
        }

    }

    println!("Done");
    // for item in rdr {
    //     match item {
    //         ftdc::RawBSONBlock::Metadata(_) => {
    //         }
    //         ftdc::RawBSONBlock::Metrics(doc) => {
    //             let mut rdr = ftdc::MetricsReader::new(&doc);


    //             for item in rdr.into_iter() {
    //                 // match item {
    //                 //     MetricsDocument::Reference(d1) => {
    //                 //         // println!("{},", serde_json::to_string_pretty(&d1.as_ref()).unwrap());
    //                 //         c+=1;
    //                 //         // println!("Ref");
    //                 //         // println!("Ref: Sample {} Metric {}", rdr.sample_count, rdr.metrics_count);
    //                 //     }
    //                 //     MetricsDocument::Metrics(d1) => {
    //                 //         break;
    //                 //     }
    //                 // };


    //                 match item {
    //                     MetricsDocument::Reference(d1) => {
    //                         // println!("ref");
    //                         analyze_ref(&d1, &mut deltas);
    //                         c += 1
    //                     }
    //                     MetricsDocument::Metrics(d1) => {
    //                         analyze_doc(&d1, &mut deltas);
    //                         // i = 1;
    //                     }
    //                 };

    //                 if( c % 100 == 0) {
    //                     println!("{}",c);
    //                 }

    //                 // let v =
    //                 // match item {
    //                 //     MetricsDocument::Reference(d1) => {
    //                 //         // println!("ref");
    //                 //         analyze_doc(&d1, &mut names)
    //                 //     }
    //                 //     MetricsDocument::Metrics(d1) => {
    //                 //         analyze_doc(&d1, &mut names)
    //                 //     }
    //                 // };
    //                 // println!("{},", serde_json::to_string_pretty(&v).unwrap());

    //                 // match item {
    //                 //     MetricsDocument::Reference(d1) => {
    //                 //         // println!("ref");
    //                 //         // analyze_doc(&d1, &mut names);
    //                 //     }
    //                 //     MetricsDocument::Metrics(_) => {
    //                 //     }
    //                 // };

    //             }

    //         }
    //     }

    //     if i == 1 {
    //         break
    //     }
    // }

    // println!("count: {}", c);

    // let keys : Vec<&String> = deltas.keys().collect();
    // let kk = keys.as_slice();
    // let count = keys[0].len();
    // for k in kk {
    //     print!("{},", k);
    // }
    // println!("end");
    // for c in 0..count {
    //     for k in kk {
    //         print!("{},", deltas[k.as_str()][c]);
    //     }
    //     println!("0")
    // }

}

    // println!("]");
    /* Basic Loop

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
*/
    /*
        let bar = ProgressBar::new(1000);
    for _ in 0..1000 {
        bar.inc(1);
        // ...
    }
    bar.finish();
        */
