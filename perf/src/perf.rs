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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = EvidentDbClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(DatabaseCreationInfo {
        name: "my-new-database".into(),
    });

    let response = client.create_database(request).await?;

    println!("RESPONSE={:?}", response);

    let request = tonic::Request::new(DatabaseRenameInfo {
        old_name: "my-new-database".into(),
        new_name: "my-other-database".into(),
    });

    let response = client.rename_database(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
