#![allow(dead_code)]
#![allow(unused_imports)]

use mongodb::{
    bson,
    bson::{doc, Bson},
    // bson::document::Document,
    error::Result,
    Client
};
use std::env;
use futures_util::StreamExt;
// use futures::future::join_all;
// use futures::join;
// use futures::try_join;
use serde::{Deserialize, Serialize};

use rand::Rng;
use std::collections::HashMap;


// async fn list_databases_and_their_collections(client: &Client) -> Result<()> {
//     println!("Databases and their collections:");
//     for db_name in client.list_database_names(None, None).await? {
//         println!("{}", db_name);
//         let db = client.database(&db_name);
//         for collection_name in db.list_collection_names(None).await? {
//             println!("\t{}", collection_name);
//         }
//     }
//     Ok(())
// }


// async fn print_collection(collection: &mongodb::Collection) -> Result<()> {
//     let mut cursor = collection.find(None, None).await?;
//     println!("The collection:");
//     // This approach queries in batches
//     while let Some(document) = cursor.next().await {
//         println!("{:#?}", document?);
//         println!();
//     }
//     Ok(())
// }

// async fn delete_database(database: &mongodb::Database) -> Result<()> {
//     database.drop(None).await?;
//     Ok(())
// }

// async fn delete_collection(collection: &mongodb::Collection) -> Result<()> {
//     collection.drop(None).await?;
//     Ok(())
// }

// async fn dump_db(db: &mongodb::Database) -> Result<()> {
//     println!("======== DUMP Start ========");
//     for collection_name in db.list_collection_names(None).await? {
//         println!(">>{}", collection_name);
//         let mut cursor = db.collection(&collection_name).find(None, None).await?;
//         while let Some(document) = cursor.next().await {
//             println!("\t{:#?}", document?);
//             println!();
//         }
//     }
//     println!("======== DUMP End ========");
//     println!();
//     Ok(())
// }
//
// async fn dump(db: &mongodb::Database) -> Result<()> {
//     println!("======== Pretty DUMP Start ========");
//     for collection_name in db.list_collection_names(None).await? {
//         println!(">>{}", collection_name);
//         let mut cursor = db.collection(&collection_name).find(None, None).await?;
//         while let Some(document) = cursor.next().await {
//             let UserData { name, .. }: UserData = bson::from_bson(Bson::Document(document?))?;
//             println!("\tBLOB of size {}", name);
//         }
//     }
//     println!("======== Pretty DUMP End ========");
//     println!();
//     Ok(())
// }

fn dump_user_data(UserData{ name, created_at, .. }: UserData) {
    print!("[at: {}, of size: {}]", created_at, name);
}

async fn dump(client: &Client) -> Result<()> {
    let db = client.database("users");
    for collection_name in db.list_collection_names(None).await? {
        println!(">>{}", collection_name);
        let cursor = db.collection(&collection_name).find(None, None).await?;
        let entries: Vec<_> = cursor.collect().await;
        println!("<<{} entries>>", entries.len());
        // let mut cursor = db.collection(&collection_name).find(None, None).await?;
        // while let Some(document) = cursor.next().await {
        //     let user_data = bson::from_bson(Bson::Document(document?))?;
        //     print!("\t");
        //     dump_user_data(user_data);
        //     println!();
        // }
    }
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
    created_at: Time,
}

fn user_data_to_doc(UserData {name, size_bytes, content, created_at}: UserData) -> bson::Document {
    doc! { "name": name, "size_bytes": size_bytes, "content": content, "created_at": created_at }
}

async fn add_data(collection: &mongodb::Collection, data: UserData) -> Result<()> {
    collection.insert_one(user_data_to_doc(data), None).await?;
    Ok(())
}

fn create_user_data(size_4_bytes: u32, created_at: Time) -> UserData {
    UserData {
        name: bytes_to_human(size_4_bytes * 4),
        size_bytes: size_4_bytes * 4,
        content: vec![25; size_4_bytes as usize],
        created_at,
    }
}



// async fn stuff(client: &Client) -> Result<()> {
    // let db = client.database("users");
    // let collection= db.collection("joe");
    // add_data(&collection, create_user_data(11 * 1024)).await?;
    // add_data(&collection, create_user_data(22 * 1024)).await?;
    // add_data(&collection, create_user_data(33 * 1024)).await?;
//     Ok(())
// }


fn to_collection(client: &Client, id: UserId) -> mongodb::Collection {
    client.database("users").collection(&format!("user{}", id))
}

// ============================================================================
// ============================================================================
// ============================================================================
#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
enum Location {
    Virginia, Belgium, Ireland, Tokyo
}


async fn create_data(collection: &mongodb::Collection, time: Time) -> Result<()>{
    // TODO: create more personalized data. Also: use some ids for data to be able to modify it
    // later
    let size = 15 * 1024;
    let data = create_user_data(size, time);
    add_data(&collection, data).await?;
    Ok(())
}

async fn delete_data(_collection: &mongodb::Collection, _time: Time) -> Result<()>{
    // TODO
    // TODO
    // TODO
    Ok(())
}



#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
enum OperationType {
    Create, Read, Update, Delete
}

impl OperationType {
    fn random() -> OperationType {
        // TODO
        // TODO
        // TODO
        // TODO
        // TODO
        // TODO
        OperationType::Create
        // let mut rng = rand::thread_rng();
        // match rng.gen_range(0..4) {
        //     0 => OperationType::Create,
        //     1 => OperationType::Read,
        //     2 => OperationType::Update,
        //     3 => OperationType::Delete,
        //     _ => panic!("Invalid range generated for OperationType"),
        // }
    }
}

#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
struct Operation {
    operation_type: OperationType,
    action_time: Time,
}

impl Operation {
    async fn perform_fake(&self, _collection: &mongodb::Collection) -> Result<()>{
        Ok(())
    }

    async fn perform(&self, collection: &mongodb::Collection) -> Result<()>{
        match self.operation_type {
            OperationType::Create => create_data(&collection, self.action_time).await?,
            OperationType::Delete => delete_data(&collection, self.action_time).await?,
            _ => println!("No-op for perform()"),
        }
        Ok(())
    }

    fn new(operation_type: OperationType, action_time: Time) -> Operation {
        Operation{ operation_type, action_time }
    }
}


type UserId = u32;
type Time = u64;

#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
struct UserRequest {
    id: UserId,
    from: Location,
    operation: Operation,
    time: Time,
}

impl UserRequest {
    fn new(id: UserId, from: Location, time: Time) -> UserRequest {
        let operation = Operation::new(OperationType::random(), time);
        UserRequest{ id, from, operation, time }
    }
}

fn create_user_request_batch(amount: usize, id: UserId, from: Location, start_time: Time) -> Vec<UserRequest> {
    let mut batch = Vec::with_capacity(amount);
    let mut time = start_time;
    for _ in 0..amount {
        batch.push(UserRequest::new(id, from, time));
        time += 1;
    }
    batch
}

fn create_user_request_stream() -> Vec<UserRequest> {
    let mut stream = Vec::with_capacity(64);

    let max_id = 16;
    let id = rand::thread_rng().gen_range(1..=max_id);
    let mut time = 1;
    {
        let amount = 10;
        let start_time = time;
        let location = Location::Tokyo;
        stream.append(&mut create_user_request_batch(amount, id, location, start_time));
        time += amount as Time;
    }
    {
        let amount = 3;
        let start_time = time;
        let location = Location::Belgium;
        stream.append(&mut create_user_request_batch(amount, id, location, start_time));
        time += amount as Time;
    }
    {
        let amount = 5;
        let start_time = time;
        let location = Location::Virginia;
        stream.append(&mut create_user_request_batch(amount, id, location, start_time));
        time += amount as Time;
    }
    {
        let amount = 2;
        let start_time = time;
        let location = Location::Tokyo;
        stream.append(&mut create_user_request_batch(amount, id, location, start_time));
        // time += amount as Time;
    }

    stream.shrink_to_fit();
    stream
}
// ============================================================================
// ============================================================================
// ============================================================================
struct Brain {
    clients: Vec<Client>,
    names: Vec<&'static str>,
    storage_ids_for_user_id: HashMap<UserId, Vec<usize>>,
}

impl Brain {
    fn storage_id_to_client(&self, id: usize) -> &Client {
        &self.clients[id]
    }

    // fn get(&self, id: usize) -> &Client {
    //     self.storage_id_to_client(id)
    // }

    async fn dump(&self) -> Result<()> {
        for (i, client) in self.clients.iter().enumerate() {
            println!("######## {} contents ########", self.names[i]);
            dump(&client).await?;
            println!("########################################");
        }
        println!();
        Ok(())
    }

    async fn collect_storage_ids_for_user_id(clients: &Vec<Client>, names: &Vec<&'static str>) -> Result<HashMap<UserId, Vec<usize>>> {
        let mut map = HashMap::new();
        for (i, client) in clients.iter().enumerate() {
            let user_ids: Vec<UserId> = client.database("users")
                .list_collection_names(None).await?
                .into_iter()
                .map(|user_str| user_str
                    .strip_prefix("user")
                    .expect("Collection name didn't start with 'user'")
                    .parse::<UserId>()
                    .expect("Failed to parse UserId from Collectio name"))
                // .for_each(|user_id| map.entry(user_id).or_insert(Vec::new()).push(i))
                .collect();

            for &id in user_ids.iter() {
                map.entry(id).or_insert(Vec::new()).push(i);
            }

            println!("Client {} contains user ids={:?}", names[i], user_ids);
        }
        println!("The final map:{:?}", map);
        Ok(map)
    }

    async fn new(clients: Vec<Client>, names: Vec<&'static str>) -> Result<Brain> {
        let storage_ids_for_user_id = Brain::collect_storage_ids_for_user_id(&clients, &names).await?;
        Ok(Brain {
            clients,
            names,
            storage_ids_for_user_id,
            // storage_ids_for_user_id: HashMap::new(),
        })
    }

    // TODO: account for FromLocation when picking
    async fn pick_new_storage(&self, _from: Location) -> Result<usize> {
        let mut min_index = 0;
        let mut min = usize::MAX;
        for (i, client) in self.clients.iter().enumerate() {
            let users = client.database("users").list_collection_names(None).await?.len();
            if users < min {
                min_index = i;
                min = users;
            }
        }
        println!("\tThe storage with minimum users is {}(with {} users)", self.names[min_index], min);
        Ok(min_index)

        // Treat all as equal for now
        // rand::thread_rng().gen_range(0..(self.clients.len()))
    }

    // TODO: account for FromLocation when selecting alternatives
    fn select_best_from(storages: &Vec<usize>, _from: Location) -> usize {
        // Select randomly first for now
        storages[0]
    }

    // fn available_storage_ids(&mut self, id: UserId, from: Location) -> Vec<usize> {
    //     let default_entry = self.pick_new_storage(from); // XXX: should be in-place, but the BC complains
    //     self.storage_ids_for_user_id.entry(id).or_insert(vec![default_entry])
    // }

    async fn handle_request(&mut self, user_request: UserRequest) -> Result<()> {
        let UserRequest{ id, from, operation, .. } = user_request;
        println!(":> Handling {:?}", &user_request);
        // let available = self.available_storage_ids(id, from);

        // let available_storage_ids = self.storage_ids_for_user_id.entry(id).or_insert(vec![default_entry]).to_vec();
        if !self.storage_ids_for_user_id.contains_key(&id) {
            let first_home = self.pick_new_storage(from).await?; // XXX: should be in-place, but the BC complains
            println!("\tThis is a new user, selecting storage {} for it", self.names[first_home]);
            self.storage_ids_for_user_id.insert(id, vec![first_home]);
        }
        let available_storage_ids = self.storage_ids_for_user_id.get(&id).unwrap();
        // println!("\tThere are {} storage variants for this request: {:?}", available_storage_ids.len(), available_storage_ids);
        let names = available_storage_ids.iter().map(|&id| self.names[id]).collect::<Vec<_>>();
        println!("\tThere are {} storage variants for this request: {:?}", available_storage_ids.len(), names);
        let selected_client_id = Brain::select_best_from(available_storage_ids, from);
        // println!("\tSelecting variant id={}", selected_client_id);
        println!("\tSelecting variant {}", self.names[selected_client_id]);
        let selected_client = self.storage_id_to_client(selected_client_id);
        operation.perform(&to_collection(selected_client, id)).await?;
        // operation.perform_fake(&to_collection(selected_client, id)).await?;

        Ok(())
    }
}
// ============================================================================
// ============================================================================
// ============================================================================













// ============================================================================
// ============================================================================
// ============================================================================
async fn reset(brain: &Brain) -> Result<()> {
    for (i, client) in brain.clients.iter().enumerate() {
        println!(":> Resetting {}", brain.names[i]);
        client.database("users").drop(None).await?
    }
    // println!("Deleting dbs...");
    // let a1 = clients[0].database("users");
    // let a2 = clients[1].database("users");
    // let a3 = clients[2].database("users");
    // let a4 = clients[3].database("users");
    // try_join!(
    //     a1.drop(None),
    //     a2.drop(None),
    //     a3.drop(None),
    //     a4.drop(None)
    // )?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut brain = Brain::new(
        vec![
        Client::with_uri_str(env::var("MONGO_MAPLE").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
        Client::with_uri_str(env::var("MONGO_LEMON").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
        Client::with_uri_str(env::var("MONGO_CHRISTMAS").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
        Client::with_uri_str(env::var("MONGO_ORANGE").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
        ],
        vec!["Maple Tree", "Lemon Tree", "Christmas Tree", "Orange Tree"]
    ).await?;

    // reset(&brain).await?;

    let requests = create_user_request_stream();
    for request in requests {
        brain.handle_request(request).await?;
    }

    brain.dump().await?;

    Ok(())
}
