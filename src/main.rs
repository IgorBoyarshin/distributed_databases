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
use futures::try_join;
use futures::future::try_join_all;
use serde::{Deserialize, Serialize};


use rand::Rng;
use std::collections::HashMap;


fn dump_user_data(UserData{ name, created_at, .. }: UserData) {
    print!("[at: {}, of size: {}]", created_at, name);
}

fn dump_user_data_to_str(UserData{ name, created_at, .. }: UserData) -> String {
    format!("[at: {}, of size: {}]", created_at, name)
}

async fn dump(client: &Client) -> Result<()> {
    let db = client.database("users");
    for collection_name in db.list_collection_names(None).await? {
        println!(">>{}", collection_name);

        // let cursor = db.collection(&collection_name).find(None, None).await?;
        // let entries: Vec<_> = cursor.collect().await;
        // println!("<<{} entries>>", entries.len());

        let mut cursor = db.collection(&collection_name).find(None, None).await?;
        while let Some(document) = cursor.next().await {
            let user_data = bson::from_bson(Bson::Document(document?))?;
            print!("\t");
            dump_user_data(user_data);
            println!();
        }
    }
    println!();
    Ok(())
}

async fn dump_to_str(client: &Client) -> Result<String> {
    let mut result = String::new();
    let db = client.database("users");
    for collection_name in db.list_collection_names(None).await? {
        result.push_str(&format!(">>{}\n", collection_name));

        // let cursor = db.collection(&collection_name).find(None, None).await?;
        // let entries: Vec<_> = cursor.collect().await;
        // println!("<<{} entries>>", entries.len());

        let mut cursor = db.collection(&collection_name).find(None, None).await?;
        while let Some(document) = cursor.next().await {
            let user_data = bson::from_bson(Bson::Document(document?))?;
            result.push('\t');
            result.push_str(&dump_user_data_to_str(user_data));
            result.push('\n');
        }
    }
    result.push('\n');
    Ok(result)
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

impl Location {
    fn random() -> Location {
        let mut rng = rand::thread_rng();
        match rng.gen_range(0..4) {
            0 => Location::Virginia,
            1 => Location::Belgium,
            2 => Location::Ireland,
            3 => Location::Tokyo,
            _ => panic!("Invalid range generated for Location"),
        }
    }
}


async fn create_data(collection: &mongodb::Collection, time: Time) -> Result<()>{
    let size = 15 * 1024;
    let data = create_user_data(size, time);
    add_data(&collection, data).await?;
    Ok(())
}

async fn delete_data(collection: &mongodb::Collection, time: Time) -> Result<()>{
    collection.delete_many(doc!{ "created_at": time }, None).await?;
    Ok(())
}

async fn read_data(collection: &mongodb::Collection, time: Time) -> Result<()>{
    collection.find_one(doc!{ "created_at": time }, None).await?;
    Ok(())
}

async fn update_data(collection: &mongodb::Collection, time: Time) -> Result<()>{
    // TODO: stay within field for "at"
    let size = 10 * 1024;
    let data = user_data_to_doc(create_user_data(size, 10 * time));
    collection.replace_one(doc!{ "created_at": time }, data, None).await?;
    Ok(())
}




#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
enum OperationType {
    Create, Read, Update, Delete
}

impl OperationType {
    fn random() -> OperationType {
        let mut rng = rand::thread_rng();
        match rng.gen_range(0..4) {
            0 => OperationType::Create,
            1 => OperationType::Read,
            2 => OperationType::Update,
            3 => OperationType::Delete,
            _ => panic!("Invalid range generated for OperationType"),
        }
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

    async fn perform(&self, collection: mongodb::Collection) -> Result<()>{
        match self.operation_type {
            OperationType::Create => create_data(&collection, self.action_time).await?,
            OperationType::Read => read_data(&collection, self.action_time).await?,
            OperationType::Update => update_data(&collection, self.action_time).await?,
            OperationType::Delete => delete_data(&collection, self.action_time).await?,
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
        let max_action_time = 10;
        let action_time = rand::thread_rng().gen_range(1..=max_action_time);
        let operation = Operation::new(OperationType::random(), action_time);
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

fn create_manual_user_request_stream() -> Vec<UserRequest> {
    let mut stream = Vec::with_capacity(64);

    let max_id = 8;
    // let id = 3;
    let id = rand::thread_rng().gen_range(1..=max_id);
    let mut time = 1;
    {
        let amount = 3;
        let start_time = time;
        let location = Location::random();
        // let location = Location::Belgium;
        stream.append(&mut create_user_request_batch(amount, id, location, start_time));
        time += amount as Time;
    }
    {
        let amount = 5;
        let start_time = time;
        let location = Location::random();
        // let location = Location::Virginia;
        stream.append(&mut create_user_request_batch(amount, id, location, start_time));
        time += amount as Time;
    }
    {
        let amount = 4;
        let start_time = time;
        let location = Location::random();
        // let location = Location::Tokyo;
        stream.append(&mut create_user_request_batch(amount, id, location, start_time));
        time += amount as Time;
    }
    {
        let amount = 2;
        let start_time = time;
        let location = Location::random();
        // let location = Location::Belgium;
        stream.append(&mut create_user_request_batch(amount, id, location, start_time));
        // time += amount as Time;
    }

    stream.shrink_to_fit();
    stream
}

fn create_random_user_request_stream(size: usize) -> Vec<UserRequest> {
    let mut stream = Vec::with_capacity(size);
    let mut time = 1;

    let mut location = Location::random();

    let max_amount = 10;
    let mut amount = rand::thread_rng().gen_range(1..=max_amount);

    let max_id = 8;
    let mut id = rand::thread_rng().gen_range(1..=max_id);

    for _ in 0..size {
        stream.push(UserRequest::new(id, location, time));

        amount -= 1;
        if amount == 0 {
            location = Location::random();
            amount = rand::thread_rng().gen_range(1..=max_amount);
            id = rand::thread_rng().gen_range(1..=max_id);
        }

        time += 1;
    }
    stream
}
// ============================================================================
// ============================================================================
// ============================================================================
type StorageId = usize;

struct Brain {
    clients: Vec<Client>,
    names: Vec<&'static str>,
    storage_ids_for_user_id: HashMap<UserId, Vec<StorageId>>,
    history: HashMap<UserId, Vec<StorageId>>,
}

impl Brain {
    fn only_one_storage_for(&self, user_id: UserId) -> bool {
        self.storage_ids_for_user_id.get(&user_id)
            .expect(&format!("Uninitialized user {} in storage_ids_for_user_id", user_id))
            .len() <= 1
    }

    fn rule_time_to_delete_storage_for(&self, storage_id: StorageId, user_id: UserId) -> bool {
        let threshold = 10;
        let mut foreign_counter = 0;
        let history = self.history.get(&user_id).expect(&format!("Uninitialized user {} in history", user_id));
        for &target in history.iter().rev() {
            if target == storage_id {
                // println!("\t[RULE Delete] For {} there WAS a request to {} during last {} queries. Will NOT delete.",
                //     user_id, self.names[storage_id], threshold);
                return false;
            }
            foreign_counter += 1;
            if foreign_counter >= threshold {
                println!("\t[RULE Delete] For {} there was NOT a request to {} during last {} queries. WILL delete.",
                    user_id, self.names[storage_id], threshold);
                return true;
            }
        }
        // There was request for storage_id, but THRESHOLD has not been reached yet
        false
    }

    fn rule_time_to_allocate_storage_for(&self, storage_id: StorageId, user_id: UserId) -> bool {
        let already_allocated_storages = self.storage_ids_for_user_id.get(&user_id)
            .expect(&format!("Uninitialized user {} in storage_ids_for_user_id", user_id));
        let is_known = |storage_id| already_allocated_storages.contains(&storage_id);
        if is_known(storage_id) {
            // Already allocated
            return false;
        }

        let depth_threshold = 8;
        let amount_threshold = 3;
        let mut amount = 0;
        let mut depth = 0;
        let history = self.history.get(&user_id).expect(&format!("Uninitialized user {} in history", user_id));
        for &target in history.iter().rev() {
            if target == storage_id {
                amount += 1;
            }
            if amount >= amount_threshold {
                println!("\t[RULE Allocate] For {} there WAS enough({}) requests that should have been to {} during last {} queries. Will allocate.",
                    user_id, amount_threshold, self.names[storage_id], depth_threshold);
                return true;
            }

            depth += 1;
            if depth >= depth_threshold {
                // println!("\t[RULE Allocate] For {} there was NOT enough({}) requests that should have been to {} during last {} queries.",
                    // user_id, amount_threshold, self.names[storage_id], depth_threshold);
                return false;
            }
        }
        // Not deep enough history
        false
    }

    async fn delete_storage_for_user(&mut self, storage_id: StorageId, user_id: UserId) -> Result<()> {
        self.clients[storage_id].database("users").collection(&format!("user{}", user_id)).drop(None).await?;
        self.storage_ids_for_user_id.get_mut(&user_id)
            .expect(&format!("Uninitialized user {} in history", user_id))
            .retain(|&x| x != storage_id);

        Ok(())
    }

    async fn allocate_storage_for_user(&mut self, storage_id: StorageId, user_id: UserId) -> Result<()> {
        println!("\t Allocating storage {} for user {}", storage_id, user_id);
        // Fetch data from any Storage with this user's data
        // ...just pick the first for now
        let storage_id_to_fetch_from = self.storage_ids_for_user_id.get(&user_id)
            .expect(&format!("Uninitialized user {} in storage_ids_for_user_id", user_id))
            [0];
        println!("\t Using {} to transfer from", storage_id_to_fetch_from);

        let user_data = to_collection(&self.clients[storage_id_to_fetch_from], user_id).find(None, None).await?
            .map(|document| bson::from_bson(Bson::Document(document.expect("Failed to parse user_data"))).expect("Failed to parse user_data"))
            .collect::<Vec<UserData>>().await;
        println!("\t Collected {} entries", user_data.len());

        // Transfer this data to the new location
        if user_data.len() > 0 {
            to_collection(&self.clients[storage_id], user_id).insert_many(user_data.into_iter().map(user_data_to_doc), None).await?;
            println!("\tFinished transfering to {}", storage_id);
        } else {
            println!("\tNo records exist for this user, so no records will be transfered");
        }

        // Update records
        self.storage_ids_for_user_id.get_mut(&user_id)
            .expect(&format!("Uninitialized user {} in storage_ids_for_user_id", user_id))
            .push(storage_id);

        Ok(())
    }



    fn storage_id_to_client(&self, id: StorageId) -> &Client {
        &self.clients[id]
    }

    async fn dump(&self) -> Result<()> {
        // Sequential
        // for (i, client) in self.clients.iter().enumerate() {
        //     println!("######## {} contents ########", self.names[i]);
        //     dump(&client).await?;
        //     println!("########################################");
        // }
        // println!();
        // Ok(())

        // Parallel
        println!("======== Dumping all databases ========");
        let (s1, s2, s3, s4) = try_join!(
            dump_to_str(&self.clients[0]),
            dump_to_str(&self.clients[1]),
            dump_to_str(&self.clients[2]),
            dump_to_str(&self.clients[3])
        )?;
        let ss = vec![s1, s2, s3, s4];
        for (i, s) in ss.into_iter().enumerate() {
            println!("######## {} contents ########", self.names[i]);
            println!("{}", s);
            println!("########################################");
        }
        println!();
        Ok(())
    }

    async fn collect_storage_ids_for_user_id(clients: &Vec<Client>, names: &Vec<&'static str>) -> Result<HashMap<UserId, Vec<StorageId>>> {
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

    // TODO: copy-pasted and modified from collect_storage_ids_for_user_id
    // TODO: can merge.....
    async fn initialize_history(clients: &Vec<Client>) -> Result<HashMap<UserId, Vec<StorageId>>> {
        let mut map = HashMap::new();
        for client in clients.iter() {
            let user_ids: Vec<UserId> = client.database("users")
                .list_collection_names(None).await?
                .into_iter()
                .map(|user_str| user_str
                    .strip_prefix("user")
                    .expect("Collection name didn't start with 'user'")
                    .parse::<UserId>()
                    .expect("Failed to parse UserId from Collectio name"))
                .collect();

            for id in user_ids.into_iter() {
                map.entry(id).or_insert(Vec::new());
            }
        }

        println!("Final known IDs for history:{:?}", map.keys());
        Ok(map)
    }

    async fn new(clients: Vec<Client>, names: Vec<&'static str>) -> Result<Brain> {
        let storage_ids_for_user_id = Brain::collect_storage_ids_for_user_id(&clients, &names).await?;
        let history = Brain::initialize_history(&clients).await?;
        Ok(Brain {
            clients,
            names,
            // storage_ids_for_user_id: HashMap::new(),
            storage_ids_for_user_id,
            // history: HashMap::new(),
            history,
        })
    }

    // XXX: prone to incorrect indexing!!!
    // XXX: prone to incorrect indexing!!!
    // XXX: prone to incorrect indexing!!!
    fn location_to_storage_id(&self, location: &Location) -> StorageId {
        match location {
            Location::Belgium => 0,
            Location::Virginia => 1,
            Location::Tokyo => 2,
            Location::Ireland => 3,
        }
    }

    fn storage_id_to_location(&self, storage_id: StorageId) -> Location {
        match storage_id {
            0 => Location::Belgium,
            1 => Location::Virginia,
            2 => Location::Tokyo,
            3 => Location::Ireland,
            _ => panic!("Invalid range for Location"),
        }
    }

    fn pick_new_storage(&self, from: Location) -> StorageId {
        // Pick Storage with best location
        return self.location_to_storage_id(&from);

        // Pick Storage with min amount of users
        // let mut min_index = 0;
        // let mut min = StorageId::MAX;
        // for (i, client) in self.clients.iter().enumerate() {
        //     let users = client.database("users").list_collection_names(None).await?.len();
        //     if users < min {
        //         min_index = i;
        //         min = users;
        //     }
        // }
        // println!("\tThe storage with minimum users is {}(with {} users)", self.names[min_index], min);
        // Ok(min_index)
    }

    fn select_best_from(&self, storages: &Vec<StorageId>, from: Location) -> StorageId {
        // Select closest based on location
        let target = self.location_to_storage_id(&from);
        for &storage_id in storages {
            if storage_id == target {
                // Found best match!
                return storage_id;
            }
        }

        // ... otherwise select randomly
        let mut rng = rand::thread_rng();
        storages[rng.gen_range(0..storages.len())]
    }

    fn registered_user(&self, user_id: UserId) -> bool {
        return self.storage_ids_for_user_id.contains_key(&user_id);
    }

    async fn handle_request(&mut self, user_request: UserRequest) -> Result<()> {
        let UserRequest{ id: user_id, from, operation, .. } = user_request;
        println!(":> Handling {:?}", &user_request);

        // Need to register this user?
        if !self.registered_user(user_id) {
            let first_home = self.pick_new_storage(from); // XXX: should be in-place, but the BC complains
            println!("\tThis is a new user, selecting storage {} for it", self.names[first_home]);

            // Initialize everything for this user
            self.storage_ids_for_user_id.insert(user_id, vec![first_home]);
            self.history.insert(user_id, vec![]);
        }

        // Determine the server (Storage) to work with
        let available_storage_ids = self.storage_ids_for_user_id.get(&user_id).unwrap();
        let names = available_storage_ids.iter().map(|&id| self.names[id]).collect::<Vec<_>>();
        println!("\tThere are {} storage variants for this request: {:?}", names.len(), names);
        let selected_storage_id = self.select_best_from(available_storage_ids, from);
        println!("\tSelecting variant {}", self.names[selected_storage_id]);

        // Perform the operation
        let client = self.storage_id_to_client(selected_storage_id);
        operation.perform(to_collection(client, user_id)).await?;
        println!("\tPerforming operation on {}", self.names[selected_storage_id]);
        // operation.perform_fake(&to_collection(client, user_id)).await?;
        // ... and maybe return result to user
        // <<< RETURN RESULT HERE >>>

        // Sync across all DBs
        // Sequential
        // {
        //     for &storage_id in available_storage_ids {
        //         if storage_id == selected_storage_id {
        //             // Have performed this operation on this DB already
        //             continue;
        //         }
        //         let client = self.storage_id_to_client(storage_id);
        //         operation.perform(&to_collection(client, user_id)).await?;
        //     }
        // }
        // Parallel
        {
            // let mut operations_temp = Vec::new();
            let mut operations = Vec::new();
            for &storage_id in available_storage_ids {
                if storage_id == selected_storage_id {
                    // Have performed this operation on this DB already
                    println!("\t\tOperation on {} has already been synced, skipping", self.names[storage_id]);
                    continue;
                }
                println!("\t\tSyncing operation on {}", self.names[storage_id]);
                let client = self.storage_id_to_client(storage_id);
                operations.push(operation.perform(to_collection(client, user_id)));
                // operations_temp.push(to_collection(client, user_id));
            }
            // for collection in operations_temp.into_iter() {
                // operations.push(operation.perform(collection));
            // }
            try_join_all(operations).await?;
        }

        // Update history
        // We use _from_ here and not _selected_ because we use this information
        // to see where the requests are wanting to go, not where we direct them
        // based on what we currently have
        let loc = self.location_to_storage_id(&from);
        self.history.get_mut(&user_id).expect(&format!("Uninitialized user {}", user_id)).push(loc);

        // Apply rules
        for storage_id in 0..4 { // clone() used for BC
            if self.rule_time_to_allocate_storage_for(storage_id, user_id) {
                println!("\t\t[RULE Allocate]: time to allocate storage {} for this user ({})", self.names[storage_id], user_id);
                self.allocate_storage_for_user(storage_id, user_id).await?;
            }
        }
        if !self.only_one_storage_for(user_id) {
            let available_storage_ids = self.storage_ids_for_user_id.get(&user_id).unwrap().clone(); // clone for BC
            for storage_id in available_storage_ids {
                if self.rule_time_to_delete_storage_for(storage_id, user_id) {
                    println!("\t\t[RULE Delete]: time to delete storage {} for this user ({})", self.names[storage_id], user_id);
                    self.delete_storage_for_user(storage_id, user_id).await?;
                }
            }
        }

        Ok(())
    }

    async fn reset(&mut self) -> Result<()> {
        println!(":> Resetting records");
        self.storage_ids_for_user_id = HashMap::new();
        self.history = HashMap::new();
        // for (i, client) in self.clients.iter().enumerate() {
        //     println!(":> Resetting {}", self.names[i]);
        //     client.database("users").drop(None).await?
        // }
        println!("Deleting dbs...");
        let a1 = self.clients[0].database("users");
        let a2 = self.clients[1].database("users");
        let a3 = self.clients[2].database("users");
        let a4 = self.clients[3].database("users");
        try_join!(
            a1.drop(None),
            a2.drop(None),
            a3.drop(None),
            a4.drop(None)
        )?;
        Ok(())
    }
}
// ============================================================================
// ============================================================================
// ============================================================================


// ============================================================================
// ============================================================================
// ============================================================================
#[tokio::main]
async fn main() -> Result<()> {
    let mut brain = Brain::new(
        vec![
        Client::with_uri_str(env::var("MONGO_MAPLE").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
        Client::with_uri_str(env::var("MONGO_LEMON").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
        Client::with_uri_str(env::var("MONGO_CHRISTMAS").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
        Client::with_uri_str(env::var("MONGO_ORANGE").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
        ],
        vec!["Maple Tree (Belgium)", "Lemon Tree (Virginia)", "Christmas Tree (Tokyo)", "Orange Tree (Ireland)"]
    ).await?;

    // brain.reset().await?;

    let requests = create_random_user_request_stream(128);
    for request in requests {
        brain.handle_request(request).await?;
    }

    brain.dump().await?;

    Ok(())
}
