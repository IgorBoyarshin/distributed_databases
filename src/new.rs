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
//                 let project_id = rand::thread_rng().gen_range(0..self.project_names.len());
//                 Some(UserRequest{ self.id, project_id, time })
//             },
//         }
//     }
// }



type ProjectId = usize;
type UserId = u32;
type Time = u128;
type Counter = usize;

#[derive(Debug, Copy, Clone, Deserialize, Serialize)]
struct UserRequest {
    id: UserId,
    project_id: ProjectId,
    // from: Location,
    // operation: Operation,
    // time: Time,
}


fn describe_user(User{ user_behavior, id, project_ids }: &User) {
    println!("User {} has behavior {:?} and projects with ids={:?}", id, user_behavior, project_ids);
}
// ============================================================================
// ============================================================================
// ============================================================================
async fn ping(client: &Client) -> Result<Time> {
    let start = time::Instant::now();
    {
        let _names = client.database("users").list_collection_names(None).await?;
        // let _s = dump_to_str(&client).await?;
    }
    let elapsed = start.elapsed().as_millis();
    Ok(elapsed)
}

async fn ping_multiple(client: &Client) -> Result<Vec<Time>> {
    let amount = 10;
    let mut times = Vec::with_capacity(amount);
    for _ in 0..amount {
        times.push(ping(&client).await?);
    }
    Ok(times)
}

// async fn ping_multiple_parallel(client: &Client) -> Result<Vec<Time>> {
//     let amount = 10;
//     let mut operations = Vec::with_capacity(amount);
//     for _ in 0..amount {
//         operations.push(ping(&client));
//     }
//     let times = try_join_all(operations).await?;
//     Ok(times)
// }
// ============================================================================
// ============================================================================
// ============================================================================
struct SimulationHyperParameters {
    request_amount: usize,
    input_intensity: f32,
    dbs: Vec<Database>,
    project_names: Vec<&'static str>,
    users: Vec<User>,
}

struct SimulationParameters {

}

struct SimulationOutput {
    duration: time::Duration,
}

async fn simulate(
        SimulationParameters{  }: SimulationParameters,
        SimulationHyperParameters{ input_intensity, request_amount, mut users, project_names, dbs }: SimulationHyperParameters) -> Result<SimulationOutput> {

    println!(":> Performing ping test...");
    let (pings_1, pings_2) = try_join!(ping_multiple(&dbs[0].client), ping_multiple(&dbs[1].client))?;
    println!("Client 1 pings = {:?}ms", pings_1);
    println!("Client 2 pings = {:?}ms", pings_2);
    println!();



    println!(":> Starting simulation...");
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
            spawner_tx.send(Some(UserRequest{ id: user.id, project_id })).unwrap();

            // Go to sleep
            let e = 2.71828f32;
            let sleep_duration = input_intensity * e.powf(-2f32 * rand::thread_rng().sample::<f32, _>(Open01));
            thread::sleep(time::Duration::from_millis(sleep_duration as u64));
            // println!("Sleeping for {}ms...", sleep_duration);

            time += 1;

            // Finish simulation?
            if time == request_amount {
                spawner_tx.send(None).unwrap();
                break;
            }
        }
    });


    // Responsible for processing UserRequests
    let simulation_start = time::Instant::now();
    let mut last_at = time::Instant::now();
    loop {
        if let Some(UserRequest{ id, project_id }) = spawner_rx.recv().expect("dead spawner_tx channel") {
            let start = time::Instant::now();
            print!("[Since last = {:>5} millis]\t", last_at.elapsed().as_millis());
            last_at = start;
            print!("At {:>6}ms got request from {} to project {:<16}", simulation_start.elapsed().as_millis(), id, project_names[project_id]);

            // Process here
            thread::sleep(time::Duration::from_millis(1));

            let elapsed_micros = start.elapsed().as_micros();
            println!("[processed in {:>5} micros]", elapsed_micros);
        } else {
            break;
        }
    }
    let simulation_duration = simulation_start.elapsed();
    println!();

    Ok(SimulationOutput{ duration: simulation_duration })
}
// ============================================================================
// ============================================================================
// ============================================================================
struct Database {
    client: Client,
    name: &'static str,
}

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
    let dbs = vec![
        Database {
            client: Client::with_uri_str(env::var("MONGO_CHRISTMAS").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
            name: "Christmas Tree",
        },
        Database {
            client: Client::with_uri_str(env::var("MONGO_ORANGE").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
            name: "Orange Tree",
        },
    ];
    let project_names = vec!["Quartz", "Pyrite", "Lapis Lazuli", "Amethyst", "Jasper", "Malachite", "Diamond"]; // @hyper
    let projects_count = project_names.len();
    let users = User::create_users(5, projects_count);

    println!(":> Describing users:");
    for user in users.iter() { describe_user(&user); }
    println!();

    let hyperparameters = SimulationHyperParameters {
        request_amount: 64,
        input_intensity: 500.0,
        dbs,
        project_names,
        users,
    };
    let parameters = SimulationParameters {

    };

    let SimulationOutput{ duration } = simulate(parameters, hyperparameters).await?;
    println!(":> Simulation finished in {} seconds", duration.as_secs());


    Ok(())
}
