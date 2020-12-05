 use mongodb::{
    bson,
    bson::{doc, Bson, document::Document},
    error::Result,
    Client
};
use std::env;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};



#[derive(Deserialize, Serialize)]
struct Pet {
    // "type" is a reserved Rust keyword, so instead of naming our 
    // struct field that, we use serde’s `rename` functionality to 
    // specify it as the name to use when serializing and 
    // deserializing.
    #[serde(rename = "type")]
    pet_type: String,
    name: String,
}

async fn list_databases_and_their_collections(client: &Client) -> Result<()> {
    println!("Databases and their collections:");
    for db_name in client.list_database_names(None, None).await? {
        println!("{}", db_name);
        let db = client.database(&db_name);
        for collection_name in db.list_collection_names(None).await? {
            println!("\t{}", collection_name);
        }
    }
    Ok(())
}



#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::with_uri_str(env::var("MONGODB_URI").expect("You must set the MONGODB_URI environment var!").as_ref()).await?;

    list_databases_and_their_collections(&client).await?;
    println!();

    let db = client.database("wild_beaver_db"); // will create if does not exist
    let collection = db.collection("wild_beaver_collection"); // will create if does not exist

    //
    // Insert stuff(will create duplicates each time the code is run)
    //

    // let new_pets = vec![
    //     doc! { "type": "dog", "name": "Rondo" },
    //     doc! { "type": "cat", "name": "Smokey" },
    //     doc! { "type": "cat", "name": "Phil" }, 
    // ];
    // collection.insert_many(new_pets, None).await?;


    //
    // Query stuff
    //

    // Specify the pattern or leave as None to select everything:
    let cursor = collection.find(None, None).await?;
    // let mut cursor = collection.find(doc! { "type": "cat" }, None).await?;
    println!("All the pets:");
    // This approach collects everything first, and then processes
    let results: Vec<Result<Document>> = cursor.collect().await;
    for document in results {
        // Can convert into the proper type, since we know it (Pet)
        let pet: Pet = bson::from_bson(Bson::Document(document?))?;
        println!("There is a {} named {}", pet.pet_type, pet.name);
    }
    println!();

    let mut cursor = collection.find(None, None).await?;
    println!("All the pets (second):");
    // This approach queries in batches
    while let Some(document) = cursor.next().await {
        // Can convert into the proper type, since we know it (Pet)
        let pet: Pet = bson::from_bson(Bson::Document(document?))?;
        println!("There is a {} named {}", pet.pet_type, pet.name);
    }

    Ok(())
}
