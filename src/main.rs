 use mongodb::{
    bson,
    bson::{doc, Bson, document::Document},
    error::Result,
    Client
};
use std::env;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};


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


async fn print_collection(collection: &mongodb::Collection) -> Result<()> {
    let mut cursor = collection.find(None, None).await?;
    println!("The collection:");
    // This approach queries in batches
    while let Some(document) = cursor.next().await {
        println!("{:#?}", document?);
        println!();
    }
    Ok(())
}

async fn delete_database(database: &mongodb::Database) -> Result<()> {
    database.drop(None).await?;
    Ok(())
}

async fn delete_collection(collection: &mongodb::Collection) -> Result<()> {
    collection.drop(None).await?;
    Ok(())
}

async fn dump_db(db: &mongodb::Database) -> Result<()> {
    println!("======== DUMP Start ========");
    for collection_name in db.list_collection_names(None).await? {
        println!(">>{}", collection_name);
        let mut cursor = db.collection(&collection_name).find(None, None).await?;
        while let Some(document) = cursor.next().await {
            println!("\t{:#?}", document?);
            println!();
        }
    }
    println!("======== DUMP End ========");
    println!();
    Ok(())
}

async fn dump(db: &mongodb::Database) -> Result<()> {
    println!("======== Pretty DUMP Start ========");
    for collection_name in db.list_collection_names(None).await? {
        println!(">>{}", collection_name);
        let mut cursor = db.collection(&collection_name).find(None, None).await?;
        while let Some(document) = cursor.next().await {
            let UserData { name, .. }: UserData = bson::from_bson(Bson::Document(document?))?;
            println!("\tBLOB of size {}", name);
        }
    }
    println!("======== Pretty DUMP End ========");
    println!();
    Ok(())
}


fn bytes_to_human(mut size: u32) -> String {
    if size < 1024 { return size.to_string() + " B"; }

    let mut letter_index = 0;
    let mut full;
    loop {
        full = size / 1024;
        if full < 1024 { break; }
        letter_index += 1;
        size /= 1024;
    }

    let mut string = full.to_string();
    let remainder = size % 1024;
    if remainder != 0 {
        string += ".";
        string += &(remainder * 10 / 1024).to_string();
    }
    string += " ";

    string += "KMG".get(letter_index..letter_index+1).expect("Size too large");
    string += "B";
    string
}





#[derive(Deserialize, Serialize)]
struct UserData {
    name: String,
    size_bytes: u32,
    content: Vec<u32>,
}

fn user_data_to_doc(UserData {name, size_bytes, content}: UserData) -> bson::Document {
    doc! { "name": name, "size_bytes": size_bytes, "content": content }
}

async fn add_data(collection: &mongodb::Collection, data: UserData) -> Result<()> {
    collection.insert_one(user_data_to_doc(data), None).await?;
    Ok(())
}

fn create_user_data(size_4_bytes: u32) -> UserData {
    UserData {
        name: bytes_to_human(size_4_bytes * 4),
        size_bytes: size_4_bytes * 4,
        content: vec![25; size_4_bytes as usize],
    }
}



async fn stuff(client: &Client) -> Result<()> {
    let db = client.database("users");
    let collection= db.collection("joe");
    add_data(&collection, create_user_data(11 * 1024)).await?;
    add_data(&collection, create_user_data(22 * 1024)).await?;
    add_data(&collection, create_user_data(33 * 1024)).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let client_maple = Client::with_uri_str(env::var("MONGO_MAPLE").expect("You must set the MONGO_<NAME> environment var!").as_ref()).await?;
    let client_lemon = Client::with_uri_str(env::var("MONGO_LEMON").expect("You must set the MONGO_<NAME> environment var!").as_ref()).await?;
    let client_christmas = Client::with_uri_str(env::var("MONGO_CHRISTMAS").expect("You must set the MONGO_<NAME> environment var!").as_ref()).await?;
    let client_orange = Client::with_uri_str(env::var("MONGO_ORANGE").expect("You must set the MONGO_<NAME> environment var!").as_ref()).await?;

    // stuff(&client_lemon).await?;
    // stuff(&client_christmas).await?;
    // stuff(&client_orange).await?;
    dump(&client_maple.database("users")).await?;
    dump(&client_orange.database("users")).await?;
    dump(&client_lemon.database("users")).await?;
    dump(&client_christmas.database("users")).await?;

    Ok(())
}
