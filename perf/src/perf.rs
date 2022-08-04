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

use com::evidentdb::evident_db_client::EvidentDbClient;
use com::evidentdb::*;
use std::time::Instant;

const OLD_NAME: &str = "my-old-database";
const NEW_NAME: &str = "my-new-database";
const DB_URL: &str = "http://[::1]:50051";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = EvidentDbClient::connect(DB_URL).await?;
    create_database(&mut client).await?;
    rename_database(&mut client).await?;
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