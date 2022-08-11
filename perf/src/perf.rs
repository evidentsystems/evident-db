pub mod com {
    pub mod evidentdb {
        tonic::include_proto!("com.evidentdb");
    }
}

pub mod io {
    pub mod cloudevents {
        pub mod v1 {
            tonic::include_proto!("io.cloudevents.v1");
        }
    }
}

use std::collections::HashMap;
use com::evidentdb::evident_db_client::EvidentDbClient;
use com::evidentdb::*;
use std::time::Instant;
use std::convert::From;
use cloudevents::{AttributesReader, Data, Event, EventBuilder, EventBuilderV10};
use cloudevents::event::AttributeValue;
use prost_types::Timestamp;
use io::cloudevents::v1::cloud_event::{Data as ProtoData};
use io::cloudevents::v1::cloud_event::CloudEventAttributeValue;
use io::cloudevents::v1::cloud_event::cloud_event_attribute_value::Attr;
use url::Url;

impl From<Event> for io::cloudevents::v1::CloudEvent {
    fn from(event: Event) -> Self {
        let data = event.data();
        return Self {
            id: event.id().into(),
            source: event.source().into(),
            spec_version: event.specversion().to_string().into(),
            r#type: event.ty().into(),
            attributes: event.iter_attributes()
                .fold(HashMap::new(),
                      |mut acc, (k, v)| {
                          acc.insert(
                              k.to_string().into(),
                              CloudEventAttributeValue{
                                  attr: Some(
                                      match v {
                                          AttributeValue::SpecVersion(v) => Attr::CeString(v.to_string().into()),
                                          AttributeValue::String(s) => Attr::CeString(s.into()),
                                          AttributeValue::URI(u) => Attr::CeUri(u.to_string().into()),
                                          AttributeValue::URIRef(u) => Attr::CeUriRef(u.to_string().into()),
                                          AttributeValue::Boolean(b) => Attr::CeBoolean(*b),
                                          AttributeValue::Integer(i) => Attr::CeInteger(*i as i32),
                                          AttributeValue::Time(t) => Attr::CeTimestamp(Timestamp {
                                              seconds: t.timestamp(),
                                              nanos: t.timestamp_subsec_nanos() as i32
                                          })
                                      }
                                  )
                              }
                          );
                          acc
                      }),
            data: match data {
                None => None,
                Some(d) => match d {
                    Data::Binary(b) => Some(ProtoData::BinaryData(b.clone().into())),
                    Data::String(s) => Some(ProtoData::TextData(s.into())),
                    Data::Json(j) => {
                        let json_bytes = serde_json::to_vec(j);
                        match json_bytes {
                            Ok(b) => Some(ProtoData::BinaryData(b.clone().into())),
                            Err(_) => None
                        }
                    }
                }
            }
        }
    }
}

const OLD_NAME: &str = "my-old-database";
const NEW_NAME: &str = "my-new-database";
const DB_URL: &str = "http://[::1]:50051";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = EvidentDbClient::connect(DB_URL).await?;
    create_database(&mut client).await?;
    rename_database(&mut client).await?;
    transact_batch(&mut client).await?;
    delete_database(&mut client).await?;

    Ok(())
}

async fn create_database(client: &mut EvidentDbClient<tonic::transport::Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let request = tonic::Request::new(DatabaseCreationInfo {
        name: OLD_NAME.into(),
    });

    let start = Instant::now();
    let response = client.create_database(request).await?;
    let duration = start.elapsed();

    println!("LATENCY={:?} RESPONSE={:?}", duration, response);

    Ok(())
}

async fn rename_database(client: &mut EvidentDbClient<tonic::transport::Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let request = tonic::Request::new(DatabaseRenameInfo {
        old_name: OLD_NAME.into(),
        new_name: NEW_NAME.into(),
    });

    let start = Instant::now();
    let response = client.rename_database(request).await?;
    let duration = start.elapsed();

    println!("LATENCY={:?} RESPONSE={:?}", duration, response);

    Ok(())
}

async fn transact_batch(client: &mut EvidentDbClient<tonic::transport::Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let event = EventBuilderV10::new()
        .id("will be overwritten")
        .source(Url::parse("http://localhost").unwrap())
        .ty("demo.event")
        .build()?;

    let request = tonic::Request::new(BatchProposal {
        database_name: NEW_NAME.into(),
        events: vec![
            ProposedEvent{
                stream: "demo-stream".into(),
                stream_state: StreamState::Any as i32,
                at_revision: None,
                event: Some(event.into()),
            },
        ].into(),
    });

    let start = Instant::now();
    let response = client.transact_batch(request).await?;
    let duration = start.elapsed();

    println!("LATENCY={:?} RESPONSE={:?}", duration, response);

    Ok(())
}

async fn delete_database(client: &mut EvidentDbClient<tonic::transport::Channel>) -> Result<(), Box<dyn std::error::Error>> {
    let request = tonic::Request::new(DatabaseDeletionInfo {
        name: NEW_NAME.into(),
    });

    let start = Instant::now();
    let response = client.delete_database(request).await?;
    let duration = start.elapsed();

    println!("LATENCY={:?} RESPONSE={:?}", duration, response);

    Ok(())
}