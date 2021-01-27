#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(destructuring_assignment)]


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

use std::collections::HashMap;

use rand::Rng;
use rand_distr::{Poisson, Distribution};
// use rand_distr::num_traits::Pow;
use rand::distributions::Open01;


use variant_count::VariantCount;


use std::thread;
use std::time;
use std::sync::mpsc::channel;
// ============================================================================
// ============================================================================
// ============================================================================



#[derive(VariantCount, Debug)]
enum UserBehavior {
    AveragedSequential {
        amount: u32,
        left: u32,
        project_id: ProjectId,
    },
    AveragedRandom,
    PreferentialMono {
        project_id: ProjectId,
    },
    PreferentialDuo {
        project_id_1: ProjectId,
        project_id_2: ProjectId,
    },
}


impl UserBehavior {
    fn gen(&mut self, projects_amount: usize) -> ProjectId {
        match self {
            UserBehavior::AveragedSequential{ amount, left, project_id } => {
                let max_amount = 10; // @hyper

                if *left == 0 {
                    *amount     = rand::thread_rng().gen_range(1..=max_amount);
                    *project_id = rand::thread_rng().gen_range(0..projects_amount);
                    *left       = *amount;
                }
                *left -= 1;

                *project_id
            },
            UserBehavior::AveragedRandom => {
                rand::thread_rng().gen_range(0..projects_amount)
            },
            UserBehavior::PreferentialMono{ project_id } =>{
                let preferential_probability = 0.7;
                let use_preferred = rand::thread_rng().sample::<f32, _>(Open01) > preferential_probability;
                if use_preferred {
                    *project_id
                } else {
                    rand::thread_rng().gen_range(0..projects_amount)
                }
            },
            UserBehavior::PreferentialDuo{ project_id_1, project_id_2 } =>{
                let preferential_probability = 0.7 / 2.0;
                let use_preferred_chance = rand::thread_rng().sample::<f32, _>(Open01);
                if use_preferred_chance <= preferential_probability {
                    *project_id_1
                } else if use_preferred_chance <= 2.0 * preferential_probability {
                    *project_id_2
                } else {
                    rand::thread_rng().gen_range(0..projects_amount)
                }
            },
        }
    }

    fn random(projects_amount: usize) -> UserBehavior {
        match rand::thread_rng().gen_range(0..UserBehavior::VARIANT_COUNT) {
            0 => UserBehavior::AveragedSequential{ amount: /* any */ 0, left: 0, project_id: /* any */ 0 },
            1 => UserBehavior::AveragedRandom,
            2 => UserBehavior::PreferentialMono{ project_id: rand::thread_rng().gen_range(0..projects_amount) },
            3 => {
                assert!(projects_amount > 1);
                let project_id_1 = rand::thread_rng().gen_range(0..projects_amount);
                let project_id_2 = loop {
                    let project_id = rand::thread_rng().gen_range(0..projects_amount);
                    if project_id != project_id_1 {
                        break project_id;
                    }
                };
                UserBehavior::PreferentialDuo{ project_id_1, project_id_2 }
            },
            _ => panic!("Bad range for UserBehavior random"),
        }
        // UserBehavior::AveragedSequential{ amount: /* any */ 0, left: 0, project_id: /* any */ 0 }
    }
}

fn generate_count_random_indices_until(count: u32, len: usize) -> Vec<ProjectId> {
    let mut vec = Vec::with_capacity(count as usize);
    let mut left = count;
    for index in 0..len {
        let remaining_indices = len - index;
        let take_probability = (left as f32) / (remaining_indices as f32);
        let take = rand::thread_rng().sample::<f32, _>(Open01) < take_probability;
        if take {
            vec.push(index);
            left -= 1;
        }
        if left == 0 { break };
    }
    vec
}

struct User {
    user_behavior: UserBehavior,
    id: UserId,
    project_ids: Vec<ProjectId>,
}

impl User {
    fn gen(&mut self) -> ProjectId {
        self.user_behavior.gen(self.project_ids.len())
    }

    fn create_users(users_amount: usize, projects_amount: usize) -> Vec<User> {
        let projects_per_user = 5; // @hyper
        let mut vec = Vec::with_capacity(users_amount);
        for id in 0..users_amount {
            let user_behavior = UserBehavior::random(projects_amount);
            let project_ids = generate_count_random_indices_until(projects_per_user, projects_amount);
            vec.push(User{ user_behavior, id: id as UserId, project_ids });
        }
        vec
    }
}

// impl Iterator for User {
//     type Item = UserRequest;
//
//     let mut time = 0;
//
//     fn next(&mut self) -> Option<UserRequest> {
//         match self {
//             UserBehavior::AveragedSequential => None,
//             UserBehavior::AveragedRandom => {
//                 let project_id = rand::thread_rng().gen_range(0..self.projects.len());
//                 Some(UserRequest{ self.id, project_id, time })
//             },
//         }
//     }
// }


enum MyEnum {
    First,
    Second{a: u32, b: u32},
}


type ProjectId = usize;
type UserId = u32;
type Time = u32;

#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
struct UserRequest {
    id: UserId,
    project_id: ProjectId,
    // from: Location,
    // operation: Operation,
    time: Time,
}


fn describe_user(User{ user_behavior, id, project_ids }: &User) {
    println!("User {} has behavior {:?} and projects with ids={:?}", id, user_behavior, project_ids);
}
// ============================================================================
// ============================================================================
// ============================================================================
async fn ping(client: &Client) -> Result<u128> {
    let start = time::Instant::now();
    {
        let _names = client.database("users").list_collection_names(None).await?;
        // let _s = dump_to_str(&client).await?;
    }
    let elapsed = start.elapsed().as_millis();
    Ok(elapsed)
}

async fn ping_multiple(client: &Client) -> Result<Vec<u128>> {
    let amount = 10;
    let mut times = Vec::with_capacity(amount);
    for _ in 0..amount {
        times.push(ping(&client).await?);
    }
    Ok(times)
}

async fn ping_multiple_parallel(client: &Client) -> Result<Vec<u128>> {
    let amount = 10;
    let mut operations = Vec::with_capacity(amount);
    for _ in 0..amount {
        operations.push(ping(&client));
    }
    let times = try_join_all(operations).await?;
    Ok(times)
}
// ============================================================================
// ============================================================================
// ============================================================================
#[derive(Deserialize, Serialize)]
struct UserData {
    name: String,
    size_bytes: u32,
    content: Vec<u32>,
    created_at: Time,
}
fn dump_user_data_to_str(UserData{ name, created_at, .. }: UserData) -> String {
    format!("[at: {}, of size: {}]", created_at, name)
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
// ============================================================================
// ============================================================================
// ============================================================================
#[tokio::main]
async fn main() -> Result<()> {
    let client1 = Client::with_uri_str(env::var("MONGO_CHRISTMAS").expect("Set the MONGO_<NAME> env!").as_ref()).await?;
    let client2 = Client::with_uri_str(env::var("MONGO_ORANGE").expect("Set the MONGO_<NAME> env!").as_ref()).await?;
    // let s = dump_to_str(&client).await?;
    // let (ping1, ping2) = try_join!(ping(&client1), ping(&client2))?;
    // println!("Client 1 ping = {}ms", ping1);
    // println!("Client 2 ping = {}ms", ping2);
    // let ((ping1, max1, min1), (ping2, max2, min2)) = try_join!(ping_average(&client1), ping_average(&client2))?;
    // println!("Client 1 ping = {}ms with min = {}ms and max = {}ms", ping1, min1, max1);
    // println!("Client 2 ping = {}ms with min = {}ms and max = {}ms", ping2, min2, max2);

    let start = time::Instant::now();
    let (pings_1, pings_2) = try_join!(ping_multiple_parallel(&client1), ping_multiple_parallel(&client2))?;
    println!("Client 1 pings = {:?}", pings_1);
    println!("Client 2 pings = {:?}", pings_2);
    println!("Elapsed = {}ms", start.elapsed().as_millis());

    if true { return Ok(()) };


    // ======== Hyper-parameters ========
    let request_stream_lambda = 500.0;
    let simulation_request_count = 100;
    let projects = vec!["Quartz", "Pyrite", "Lapis Lazuli", "Amethyst", "Jasper", "Malachite", "Diamond"]; // @hyper
    let projects_count = projects.len();
    let user_amount = 5;
    let mut users = User::create_users(user_amount, projects_count);

    for user in users.iter() { describe_user(&user); }


    let (spawner_tx, spawner_rx) = channel();

    // Responsible for spawning UserRequests
    thread::spawn(move|| {
        let mut time = 0;
        loop {
            // Pick random user
            let len = users.len();
            let user = &mut users[rand::thread_rng().gen_range(0..len)];

            // Pick project for this User according to his Strategy
            let project_id = user.gen();

            // Send for execution
            spawner_tx.send(Some(UserRequest{ id: user.id, project_id, time })).unwrap();

            // Go to sleep
            let e = 2.71828f32;
            let sleep_duration = request_stream_lambda * e.powf(-2f32 * rand::thread_rng().sample::<f32, _>(Open01));
            thread::sleep(time::Duration::from_millis(sleep_duration as u64));
            // println!("Sleeping for {}ms...", sleep_duration);

            time += 1;

            // Finish simulation?
            if time == simulation_request_count {
                spawner_tx.send(None).unwrap();
                break;
            }
        }
    });


    // Responsible for processing UserRequests
    let mut last_at = time::Instant::now();
    loop {
        if let Some(UserRequest{ id, project_id, time }) = spawner_rx.recv().expect("dead spawner_tx channel") {
            let start = time::Instant::now();
            print!("[Since last = {:>5} millis]\t", last_at.elapsed().as_millis());
            last_at = start;
            print!("At [{:>3}] got request from {} to project {:>16}", time, id, projects[project_id]);

            // Process here
            thread::sleep(time::Duration::from_millis(1));

            let elapsed_micros = start.elapsed().as_micros();
            println!("\t[processed in {:>5} micros]", elapsed_micros);
        } else {
            println!(":> Simulation finished (requests stream exhausted)");
            break;
        }
    }


    Ok(())
}
