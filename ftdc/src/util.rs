use bson::doc;
use bson::spec::BinarySubtype;
use bson::Binary;
use bson::Bson;
use bson::Document;
use bson::RawArrayBuf;
use bson::RawBson;
use bson::RawBsonRef;
use bson::RawDocument;
use bson::RawDocumentBuf;
use bson::RawJavaScriptCodeWithScope;
use bson::Regex;
use chrono::DateTime;
use chrono::Utc;

pub(crate) fn gen_metadata_document(doc: &Document, date: DateTime<Utc>) -> Document {
    doc! {
        "_id" : date,
        "type": 0,
        "doc" : doc
    }
}

pub(crate) fn gen_metrics_document(chunk: &[u8], date: DateTime<Utc>) -> Document {
    doc! {
        "_id" : date,
        "type": 1,
        "data" : bson::binary::Binary{ subtype: BinarySubtype::Generic, bytes: chunk.to_vec() }
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

pub fn extract_metrics(doc: &Document) -> Vec<u64> {
    let mut metrics: Vec<u64> = Vec::new();
    extract_metrics_int(doc, &mut metrics);
    metrics
}

fn extract_metrics_bson_raw_int(value: &RawBsonRef, metrics: &mut Vec<u64>) {
    match value {
        &RawBsonRef::Double(f) => {
            metrics.push(f as u64);
        }
        &RawBsonRef::Int64(f) => {
            metrics.push(f as u64);
        }
        &RawBsonRef::Int32(f) => {
            metrics.push(f as u64);
        }
        &RawBsonRef::Decimal128(_) => {
            panic!("Decimal128 not implemented")
        }
        &RawBsonRef::Boolean(f) => {
            metrics.push(f as u64);
        }
        &RawBsonRef::DateTime(f) => {
            metrics.push(f.timestamp_millis() as u64);
        }
        &RawBsonRef::Timestamp(f) => {
            metrics.push(f.time as u64);
            metrics.push(f.increment as u64);
        }
        RawBsonRef::Document(o) => {
            extract_metrics_raw_int(o, metrics);
        }
        RawBsonRef::Array(a) => {
            for b in *a {
                extract_metrics_bson_raw_int(&b.expect("valid array element"), metrics);
            }
        }

        &RawBsonRef::JavaScriptCode(_) => {}
        &RawBsonRef::JavaScriptCodeWithScope(_) => {}
        &RawBsonRef::Binary(_) => {}
        &RawBsonRef::ObjectId(_) => {}
        &RawBsonRef::DbPointer(_) => {}
        &RawBsonRef::MaxKey | &RawBsonRef::MinKey | &RawBsonRef::Undefined => {}
        &RawBsonRef::String(_)
        | &RawBsonRef::Null
        | &RawBsonRef::Symbol(_)
        | &RawBsonRef::RegularExpression(_) => {}
    }
}

fn extract_metrics_raw_int(doc: &RawDocument, metrics: &mut Vec<u64>) {
    for item in doc.iter() {
        let value = item.expect("Valid raw bson").1;
        extract_metrics_bson_raw_int(&value, metrics);
    }
}

pub fn extract_metrics_raw(doc: &RawDocument) -> Vec<u64> {
    let mut metrics: Vec<u64> = Vec::new();
    extract_metrics_raw_int(doc, &mut metrics);
    metrics
}

fn concat2(a1: &str, a2: &str) -> String {
    let mut s = a1.to_string();
    s.push_str(a2);
    s
}

fn concat3(a1: &str, a2: &str, a3: &str) -> String {
    let mut s = a1.to_string();
    s.push_str(a2);
    s.push('.');
    s.push_str(a3);
    s
}

pub enum MetricType {
    Double,
    Int64,
    Int32,
    Boolean,
    DateTime,
    Timestamp,
}

pub struct MetricTypeInfo {
    pub name: String,
    pub metric_type: MetricType,
}

fn extract_metrics_paths_bson_int(
    value: &(&String, &Bson),
    prefix: &str,
    metrics: &mut Vec<MetricTypeInfo>,
) {
    let prefix_dot_str = prefix.to_string() + ".";
    let prefix_dot = prefix_dot_str.as_str();
    let name = &value.0;
    match value.1 {
        &Bson::Double(_) => {
            let a1 = concat2(prefix_dot, name.as_str());
            metrics.push(MetricTypeInfo {
                name: a1,
                metric_type: MetricType::Double,
            });
        }
        &Bson::Int64(_) => {
            let a1 = concat2(prefix_dot, name.as_str());
            metrics.push(MetricTypeInfo {
                name: a1,
                metric_type: MetricType::Int64,
            });
        }
        &Bson::Int32(_) => {
            let a1 = concat2(prefix_dot, name.as_str());
            metrics.push(MetricTypeInfo {
                name: a1,
                metric_type: MetricType::Int32,
            });
        }
        &Bson::Boolean(_) => {
            let a1 = concat2(prefix_dot, name.as_str());
            metrics.push(MetricTypeInfo {
                name: a1,
                metric_type: MetricType::Boolean,
            });
        }
        &Bson::DateTime(_) => {
            let a1 = concat2(prefix_dot, name.as_str());
            metrics.push(MetricTypeInfo {
                name: a1,
                metric_type: MetricType::DateTime,
            });
        }
        &Bson::Decimal128(_) => {
            panic!("Decimal128 not implemented")
        }
        &Bson::Timestamp(_) => {
            metrics.push(MetricTypeInfo {
                name: concat3(prefix_dot, name.as_str(), "t"),
                metric_type: MetricType::Timestamp,
            });
            metrics.push(MetricTypeInfo {
                name: concat3(prefix_dot, name.as_str(), "i"),
                metric_type: MetricType::Timestamp,
            });
        }
        Bson::Document(o) => {
            extract_metrics_paths_int(o, concat2(prefix_dot, name.as_str()).as_str(), metrics);
        }
        Bson::Array(a) => {
            for b in a.iter().enumerate() {
                extract_metrics_paths_bson_int(
                    &(*name, b.1),
                    concat3(prefix_dot, name.as_str(), &b.0.to_string()).as_str(),
                    metrics,
                );
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

fn extract_metrics_paths_int(doc: &Document, prefix: &str, metrics: &mut Vec<MetricTypeInfo>) {
    for item in doc {
        // if item.0 == "/boot/efi" {
        //     println!("MCB")
        // }
        extract_metrics_paths_bson_int(&item, prefix, metrics);
    }
}

pub fn extract_metrics_paths(doc: &Document) -> Vec<MetricTypeInfo> {
    let mut metrics: Vec<MetricTypeInfo> = Vec::new();
    extract_metrics_paths_int(doc, "", &mut metrics);
    metrics
}

fn extract_metrics_paths_bson_raw_int(
    value: (&str, RawBsonRef),
    prefix: &str,
    metrics: &mut Vec<MetricTypeInfo>,
) {
    let prefix_dot_str = prefix.to_string() + ".";
    let prefix_dot = prefix_dot_str.as_str();
    let name = &value.0;
    match value.1 {
        RawBsonRef::Double(_) => {
            let a1 = concat2(prefix_dot, name);
            metrics.push(MetricTypeInfo {
                name: a1,
                metric_type: MetricType::Double,
            });
        }
        RawBsonRef::Int64(_) => {
            let a1 = concat2(prefix_dot, name);
            metrics.push(MetricTypeInfo {
                name: a1,
                metric_type: MetricType::Int64,
            });
        }
        RawBsonRef::Int32(_) => {
            let a1 = concat2(prefix_dot, name);
            metrics.push(MetricTypeInfo {
                name: a1,
                metric_type: MetricType::Int32,
            });
        }
        RawBsonRef::Boolean(_) => {
            let a1 = concat2(prefix_dot, name);
            metrics.push(MetricTypeInfo {
                name: a1,
                metric_type: MetricType::Boolean,
            });
        }
        RawBsonRef::DateTime(_) => {
            let a1 = concat2(prefix_dot, name);
            metrics.push(MetricTypeInfo {
                name: a1,
                metric_type: MetricType::DateTime,
            });
        }
        RawBsonRef::Decimal128(_) => {
            panic!("Decimal128 not implemented")
        }
        RawBsonRef::Timestamp(_) => {
            metrics.push(MetricTypeInfo {
                name: concat3(prefix_dot, name, "t"),
                metric_type: MetricType::Timestamp,
            });
            metrics.push(MetricTypeInfo {
                name: concat3(prefix_dot, name, "i"),
                metric_type: MetricType::Timestamp,
            });
        }
        RawBsonRef::Document(o) => {
            extract_metrics_paths_raw_int(o, concat2(prefix_dot, name).as_str(), metrics);
        }
        RawBsonRef::Array(a) => {
            let mut idx = 0;
            for b in a {
                extract_metrics_paths_bson_raw_int(
                    (*name, b.expect("valid raw bson array element in extract")),
                    concat3(prefix_dot, name, &idx.to_string()).as_str(),
                    metrics,
                );
                idx += 1;
            }
        }

        RawBsonRef::JavaScriptCode(_) => {}
        RawBsonRef::JavaScriptCodeWithScope(_) => {}
        RawBsonRef::Binary(_) => {}
        RawBsonRef::ObjectId(_) => {}
        RawBsonRef::DbPointer(_) => {}
        RawBsonRef::MaxKey | RawBsonRef::MinKey | RawBsonRef::Undefined => {}

        RawBsonRef::String(_)
        | RawBsonRef::Null
        | RawBsonRef::Symbol(_)
        | RawBsonRef::RegularExpression(_) => {}
    }
}

fn extract_metrics_paths_raw_int(
    doc: &RawDocument,
    prefix: &str,
    metrics: &mut Vec<MetricTypeInfo>,
) {
    for item in doc {
        extract_metrics_paths_bson_raw_int(
            item.expect("Valid bson for extract metrics"),
            prefix,
            metrics,
        );
    }
}

pub fn extract_metrics_paths_raw(doc: &RawDocument) -> Vec<MetricTypeInfo> {
    let mut metrics: Vec<MetricTypeInfo> = Vec::new();
    extract_metrics_paths_raw_int(doc, "", &mut metrics);
    metrics
}

fn fill_to_bson_int(ref_field: (&String, &Bson), it: &mut dyn Iterator<Item = &u64>) -> Bson {
    match ref_field.1 {
        &Bson::Double(_) => Bson::Double(*it.next().unwrap() as f64),
        &Bson::Int64(_) => Bson::Int64(*it.next().unwrap() as i64),
        &Bson::Int32(_) => Bson::Int32(*it.next().unwrap() as i32),
        &Bson::Boolean(_) => Bson::Boolean(*it.next().unwrap() != 0),
        &Bson::DateTime(_) => {
            let p1 = it.next().unwrap();

            Bson::DateTime(bson::DateTime::from_millis(*p1 as i64))
        }
        Bson::Timestamp(_) => {
            let p1 = it.next().unwrap();
            let p2 = it.next().unwrap();

            Bson::Timestamp(bson::Timestamp {
                time: *p1 as u32,
                increment: *p2 as u32,
            })
        }
        &Bson::Decimal128(_) => {
            panic!("Decimal128 not implemented")
        }
        Bson::Document(o) => {
            let mut doc_nested = Document::new();
            for ref_field2 in o {
                fill_document_bson_int(ref_field2, it, &mut doc_nested);
            }
            Bson::Document(doc_nested)
        }
        Bson::Array(a) => {
            let mut arr: Vec<Bson> = Vec::new();

            let c = "ignore".to_string();

            for b in a {
                let tuple = (&c, b);
                arr.push(fill_to_bson_int(tuple, it));
            }

            Bson::Array(arr)
        }

        Bson::JavaScriptCode(a) => Bson::JavaScriptCode(a.to_string()),
        Bson::JavaScriptCodeWithScope(a) => Bson::JavaScriptCodeWithScope(a.clone()),
        Bson::Binary(a) => Bson::Binary(a.clone()),
        Bson::ObjectId(a) => Bson::ObjectId(*a),
        Bson::String(a) => Bson::String(a.to_string()),
        &Bson::Null => Bson::Null,
        Bson::Symbol(a) => Bson::Symbol(a.to_string()),
        Bson::RegularExpression(a) => Bson::RegularExpression(a.clone()),
        Bson::DbPointer(a) => Bson::DbPointer(a.clone()),
        &Bson::MaxKey => Bson::MaxKey,
        &Bson::MinKey => Bson::MinKey,
        &Bson::Undefined => Bson::Undefined,
    }
}

fn fill_document_bson_int(
    ref_field: (&String, &Bson),
    it: &mut dyn Iterator<Item = &u64>,
    doc: &mut Document,
) {
    doc.insert(ref_field.0.to_string(), fill_to_bson_int(ref_field, it));
}

pub fn fill_document(ref_doc: &Document, metrics: &[u64]) -> Document {
    let mut doc = Document::new();

    let mut cur = metrics.iter();

    for item in ref_doc {
        fill_document_bson_int(item, &mut cur, &mut doc);
    }

    doc
}

fn fill_to_raw_bson_int(
    ref_field: (&str, RawBsonRef),
    it: &mut dyn Iterator<Item = &u64>,
) -> RawBson {
    match ref_field.1 {
        RawBsonRef::Double(_) => RawBson::Double(*it.next().unwrap() as f64),
        RawBsonRef::Int64(_) => RawBson::Int64(*it.next().unwrap() as i64),
        RawBsonRef::Int32(_) => RawBson::Int32(*it.next().unwrap() as i32),
        RawBsonRef::Boolean(_) => RawBson::Boolean(*it.next().unwrap() != 0),
        RawBsonRef::DateTime(_) => {
            let p1 = it.next().unwrap();

            RawBson::DateTime(bson::DateTime::from_millis(*p1 as i64))
        }
        RawBsonRef::Timestamp(_) => {
            let p1 = it.next().unwrap();
            let p2 = it.next().unwrap();

            RawBson::Timestamp(bson::Timestamp {
                time: *p1 as u32,
                increment: *p2 as u32,
            })
        }
        RawBsonRef::Decimal128(_) => {
            panic!("Decimal128 not implemented")
        }
        RawBsonRef::Document(o) => {
            let mut doc_nested = RawDocumentBuf::new();
            for ref_field2 in o {
                fill_document_bson_raw_int(ref_field2.expect("valid field"), it, &mut doc_nested);
            }
            RawBson::Document(doc_nested)
        }
        RawBsonRef::Array(a) => {
            let mut arr = RawArrayBuf::new();

            let c = "ignore".to_string();
            let c_str: &str = &c;

            for b in a {
                let tuple = (c_str, b.expect("valid array field for fill"));
                arr.push(fill_to_raw_bson_int(tuple, it));
            }

            RawBson::Array(arr)
        }

        RawBsonRef::JavaScriptCode(a) => RawBson::JavaScriptCode(a.to_string()),
        RawBsonRef::JavaScriptCodeWithScope(a) => {
            RawBson::JavaScriptCodeWithScope(RawJavaScriptCodeWithScope {
                code: a.code.to_owned(),
                scope: a.scope.to_owned(),
            })
        }
        RawBsonRef::Binary(a) => RawBson::Binary(Binary {
            subtype: a.subtype,
            bytes: a.bytes.to_vec(),
        }),
        RawBsonRef::ObjectId(a) => RawBson::ObjectId(a),
        RawBsonRef::String(a) => RawBson::String(a.to_string()),
        RawBsonRef::Null => RawBson::Null,
        RawBsonRef::Symbol(a) => RawBson::Symbol(a.to_string()),
        RawBsonRef::RegularExpression(a) => RawBson::RegularExpression(Regex {
            pattern: a.pattern.to_owned(),
            options: a.options.to_owned(),
        }),
        RawBsonRef::DbPointer(_a) => todo!("Not supported"), //NOTE: bson crate cannot convert from RawDbPointer to DBPointer so we need the todo!,
        RawBsonRef::MaxKey => RawBson::MaxKey,
        RawBsonRef::MinKey => RawBson::MinKey,
        RawBsonRef::Undefined => RawBson::Undefined,
    }
}

fn fill_document_bson_raw_int(
    ref_field: (&str, RawBsonRef),
    it: &mut dyn Iterator<Item = &u64>,
    doc: &mut RawDocumentBuf,
) {
    doc.append(ref_field.0, fill_to_raw_bson_int(ref_field, it));
}

pub fn fill_document_raw(ref_doc: &RawDocument, metrics: &[u64]) -> RawDocumentBuf {
    let mut doc = RawDocumentBuf::new();

    let mut cur = metrics.iter();

    for item in ref_doc {
        fill_document_bson_raw_int(item.expect("valid bson for fill"), &mut cur, &mut doc);
    }

    doc
}
