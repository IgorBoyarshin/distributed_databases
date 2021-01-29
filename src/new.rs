#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(destructuring_assignment)]
#![feature(duration_zero)]
#![feature(duration_saturating_ops)]


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


// use crossbeam_utils;
use futures::executor::block_on;
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
                let max_amount = 10;

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
    }
}
// ============================================================================
// ============================================================================
// ============================================================================
struct User {
    user_behavior: UserBehavior,
    id: UserId,
    project_ids: Vec<ProjectId>,
}

impl User {
    fn gen(&mut self) -> ProjectId {
        self.user_behavior.gen(self.project_ids.len())
    }

    fn create_users(users_amount: usize, projects_amount: usize, projects_per_user: usize) -> Vec<User> {
        let mut vec = Vec::with_capacity(users_amount);
        for id in 0..users_amount {
            let user_behavior = UserBehavior::random(projects_amount);
            let project_ids = generate_count_random_indices_until(projects_per_user, projects_amount);
            vec.push(User{ user_behavior, id: id as UserId, project_ids });
        }
        vec
    }
}

fn generate_count_random_indices_until(count: usize, len: usize) -> Vec<ProjectId> {
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

fn describe_user(User{ user_behavior, id, project_ids }: &User) {
    println!("User {} has behavior {:?} and projects with ids={:?}", id, user_behavior, project_ids);
}
// ============================================================================
// ============================================================================
// ============================================================================
type ProjectId = usize;
type UserId = u32;
type Time = u128;
type Counter = usize;

// #[derive(Debug, Copy, Clone, Deserialize, Serialize)]
#[derive(Debug, Copy, Clone)]
struct UserRequest {
    id: UserId,
    project_id: ProjectId,
    created_at: time::Instant,
    received_at: Option<time::Instant>,
    assigned_at: Option<time::Instant>,
    finished_at: Option<time::Instant>,
    processed_at_worker: Option<usize>,
    ping_lasted: Option<time::Duration>,
    // from: Location,
    // operation: Operation,
    // time: Time,
}

impl UserRequest {
    fn new(id: UserId, project_id: ProjectId, created_at: time::Instant) -> UserRequest {
        UserRequest {
            id, project_id, created_at,
            received_at: None,
            assigned_at: None,
            finished_at: None,
            processed_at_worker: None,
            ping_lasted: None,
        }
    }
}
// ============================================================================
// ============================================================================
// ============================================================================
async fn ping(client: &Client) -> Result<time::Duration> {
    let start = time::Instant::now();
    {
        let _names = client.database("users").list_collection_names(None).await?;
        // let _s = dump_to_str(&client).await?;
    }
    Ok(start.elapsed())
}

async fn ping_multiple(client: &Client) -> Result<Vec<time::Duration>> {
    let amount = 10;
    let mut times = Vec::with_capacity(amount);
    for _ in 0..amount {
        times.push(ping(&client).await?);
    }
    Ok(times)
}

async fn determine_ping(client: &Client) -> Result<time::Duration> {
    // Warm up
    let count = 1;
    for _ in 0..count {
        // Ignore output
        let _duration = ping(&client).await?;
        // println!("Have warm {}", _duration.as_millis());
    }
    // Average
    let count = 3;
    let mut sum = time::Duration::ZERO;
    for _ in 0..count {
        let duration = ping(&client).await?;
        sum = sum.saturating_add(duration);
        // println!("Have avg {}", duration.as_millis());
    }
    Ok(sum.div_f32(count as f32))
}
// ============================================================================
// ============================================================================
// ============================================================================
struct SimulationHyperParameters {
    request_amount: usize,
    input_intensity: Option<f32>,
    dbs: Vec<Database>,
    project_names: Vec<&'static str>,
    users: Vec<User>,
    first_sort_by_ping: bool,
}

struct SimulationParameters {
    spread_rate: f32,
    decay_rate: f32,
}

struct SimulationOutput {
    duration: time::Duration,
    processed_user_requests: Vec<UserRequest>,
}

async fn simulate(
        SimulationParameters{ spread_rate: _, decay_rate: _ }: SimulationParameters,
        SimulationHyperParameters{ input_intensity, request_amount, mut users, project_names: _, mut dbs, first_sort_by_ping }: SimulationHyperParameters) -> Result<SimulationOutput> {
    println!(":> Preparing simulation");
    let (spawner_tx, spawner_rx) = channel();

    if first_sort_by_ping {
        println!(":> Sorting DBs based on ping...");
        // Collect ping in parallel
        // let pings = try_join_all(dbs.iter().map(|db| determine_ping(&db.client))).await?.into_iter().map(|d| d.as_millis()).collect::<Vec<_>>();
        // Parallel ping seems to give skewed results. As this procedure is not
        // that long and is done only once, we don't mind waiting a bit for sequential ping.

        // Collect ping sequentiall y
        let mut pings = Vec::with_capacity(dbs.len());
        for db in dbs.iter() {
            pings.push(determine_ping(&db.client).await?.as_millis());
        }

        dbs = {
            let mut dbs = dbs.into_iter().enumerate().collect::<Vec<_>>();
            dbs.sort_by(|&(i1, _), &(i2, _)| pings[i1].cmp(&pings[i2]));
            print!("Will use such order: ");
            for (i, db) in dbs.iter() {
                print!("{}({}ms); ", db.name, pings[*i]);
            }
            println!();
            dbs.into_iter().map(|(_, db)| db).collect::<Vec<_>>()
        };
        println!();
    }
 
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
            let request = UserRequest::new(user.id, project_id, time::Instant::now());
            spawner_tx.send(Some(request)).unwrap();

            if let Some(input_intensity) = input_intensity {
                // Go to sleep
                let sleep_duration_millis = -1000.0 * rand::thread_rng().sample::<f32, _>(Open01).ln() / input_intensity;
                thread::sleep(time::Duration::from_millis(sleep_duration_millis as u64));
            }

            time += 1;

            // Finish simulation?
            if time == request_amount {
                spawner_tx.send(None).unwrap();
                break;
            }
        }
    });


    // Responsible for processing UserRequests
    println!(":> Starting simulation");
    let simulation_start = time::Instant::now();
    let mut workers: Vec<Option<UserRequest>> = Vec::with_capacity(dbs.len());
    for _ in 0..dbs.len() {
        workers.push(None); // all are available at the beginning
    }
    let mut processed_user_requests = Vec::new();
    let (counter_tx, counter_rx) = channel();
    crossbeam_utils::thread::scope(|scope| {
        let mut collect_result = |workers: &mut Vec<Option<UserRequest>>, (finished_worker, finished_at, ping_lasted)| {
            let worker: &mut Option<UserRequest> = &mut workers[finished_worker];
            assert!(worker.is_some()); // must be not available yet. WTF with type???
            let mut user_request = worker.take().unwrap();
            user_request.finished_at = Some(finished_at);
            user_request.ping_lasted = Some(ping_lasted);
            processed_user_requests.push(user_request);
        };

        let mut iteration = 0;
        while let Some(mut user_request) = spawner_rx.recv().expect("dead spawner_tx channel") {
            print!("\rIteration [{}]", iteration);
            iteration += 1;

            let start = time::Instant::now();
            user_request.received_at = Some(start);
            if input_intensity.is_none() {
                // Means we want to test the maximum system throughput, thus it
                // makes sense to set the creation_time as the reception_time (now),
                // even though *technically* they were all created almost simultaniously
                // a long time ago.
                // In this scenario, the processing intensity of the system should
                // be equal both when calculating the actual value (processed amount
                // divided by time taken) and when calculating the theoretical value
                // (using average request processing time).
                user_request.created_at = start;
            }

            // Check for workers that have finished
            let exist_available_worker = workers.iter().any(|&r| r.is_none());
            if !exist_available_worker {
                // ... then unconditionally must wait for at least one to finish
                // let waiting_start = time::Instant::now();
                let result = counter_rx.recv().expect("counter rx logic failure");
                // println!("No available workers, waiting for {} micros...", waiting_start.elapsed().as_micros());
                collect_result(&mut workers, result);
            }
            // Try to collect others but do not block
            while let Ok(result) = counter_rx.try_recv() {
                collect_result(&mut workers, result);
            }

            // Choose worker to do the job
            let available_workers = workers.iter().enumerate()
                .filter(|(_i, &user_request)| user_request.is_none())
                .collect::<Vec<_>>(); // TODO: do we need Vec???
            assert!(available_workers.len() > 0); // because we have specifically waited for at least one to finish
            // XXX: for now, pick the first available worker, as they are sorted
            // lowest to highest response time.
            let (chosen_worker, _) = available_workers[0];
            // println!("Choosing worker {}", chosen_worker);
            user_request.assigned_at = Some(time::Instant::now());
            user_request.processed_at_worker = Some(chosen_worker);
            workers[chosen_worker] = Some(user_request);
            let inner_counter_tx = counter_tx.clone();
            let Database { client, .. } = &dbs[chosen_worker];
            let _t = scope.spawn(move |_| {
                // Process here
                let ping_lasted = block_on(ping(&client)).expect("failed ping");

                let finish = time::Instant::now();
                // Report that this worker has finished and is free now
                inner_counter_tx.send((chosen_worker, finish, ping_lasted)).expect("broken channel");
            });
        }
        // println!(); // iteration print reset

        // Collect the remaining ones still trapped in the system
        let remaining_amount = workers.iter().filter(|&r| r.is_some()).count();
        for _ in 0..remaining_amount {
            print!("\rIteration [{}]", iteration);
            iteration += 1;

            let result = counter_rx.recv().expect("counter rx logic failure");
            collect_result(&mut workers, result);
        }
        println!(); // iteration print reset
    }).expect("crossbeam scope unwrap failure");

    let simulation_duration = simulation_start.elapsed();
    println!();

    Ok(SimulationOutput{ duration: simulation_duration, processed_user_requests })
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
async fn get_hyperparameters() -> Result<SimulationHyperParameters> {
    let dbs = vec![
        Database {
            client: Client::with_uri_str(env::var("MONGO_CHRISTMAS").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
            name: "Christmas Tree",
        },
        Database {
            client: Client::with_uri_str(env::var("MONGO_ORANGE").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
            name: "Orange Tree",
        },
        Database {
            client: Client::with_uri_str(env::var("MONGO_LEMON").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
            name: "Lemon Tree",
        },
        Database {
            client: Client::with_uri_str(env::var("MONGO_MAPLE").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
            name: "Maple Tree",
        },
    ];
    let project_names = vec!["Quartz", "Pyrite", "Lapis Lazuli", "Amethyst", "Jasper", "Malachite", "Diamond"];

    let projects_per_user = 4;
    let projects_count = project_names.len();
    // Users are created here and not inside simulation based on hyperparameters
    // because there is an element of random in creation (e.g. Behavior), and
    // we would like all simulation to be conducted with the same set of users.
    let users = User::create_users(5, projects_count, projects_per_user);

    // let processing_intensity = 4.0 / (0.042 + 0.057 + 0.132 + 0.264);
    let processing_intensity = 1.0/0.042 + 1.0/0.057 + 1.0/0.132 + 1.0/0.264;
    Ok(SimulationHyperParameters {
        request_amount: 2048,
        // input_intensity: None,
        input_intensity: Some(0.95 * processing_intensity),
        // input_intensity: Some(1.05 * processing_intensity),
        dbs,
        project_names,
        users,
        first_sort_by_ping: true,
    })
}

fn get_parameters() -> SimulationParameters {
    SimulationParameters {
        spread_rate: 2.0,
        decay_rate: 3.0,
    }
}

fn describe_simulation_hyperparameters(SimulationHyperParameters{ users, .. }: &SimulationHyperParameters) {
    println!(":> Users:");
    for user in users.iter() { describe_user(&user); }
    println!();
}

fn describe_simulation_output(SimulationOutput{ duration, processed_user_requests }: &SimulationOutput) {
    /*
     * The (assigned_at - received_at) time generally can never be greater than
     * the waiting time for the fastest worker, so this metric is useless as a
     * waiting_time metric.
     */
    println!(":> Processed UserRequests statistics:");
    let mut average_total_time = 0;
    let mut average_waiting_time = 0;
    let mut worker_usage_count = Vec::new(); // TODO: emperically determines amount of workers
    for UserRequest{ created_at, received_at, assigned_at, finished_at, processed_at_worker, ping_lasted, .. } in processed_user_requests {
        let received_at = received_at.expect("empty time Option while describing processed request");
        let assigned_at = assigned_at.expect("empty time Option while describing processed request");
        let finished_at = finished_at.expect("empty time Option while describing processed request");
        let processed_at_worker = processed_at_worker.expect("empty num Option while describing processed request");
        let ping_lasted = ping_lasted.expect("empty ping Option while describing processed request");

        let waiting_time = assigned_at.duration_since(*created_at).as_millis();
        let total_time = finished_at.duration_since(received_at).as_millis();
        average_total_time += total_time;
        average_waiting_time += waiting_time;
        println!("Waited for {:>7} millis, processed in {:>8} millis, processed at {}, ping lasted {}ms",
            waiting_time,
            total_time,
            processed_at_worker,
            ping_lasted.as_millis(),
        );

        while processed_at_worker >= worker_usage_count.len() {
            worker_usage_count.push(0);
        }
        worker_usage_count[processed_at_worker] += 1;
    }
    average_total_time   /= processed_user_requests.len() as u128;
    average_waiting_time /= processed_user_requests.len() as u128;
    println!(":> Simulation finished in {} seconds", duration.as_secs());
    let processing_intensity = 1000.0 * processed_user_requests.len() as f32 / duration.as_millis() as f32;
    println!(":> Simulation processing intensity is {} requests per second ({} millis per request)",
        processing_intensity, (1000.0 / processing_intensity) as u32);
    println!(":> Average total processing time = {} millis", average_total_time);
    println!(":> Average waiting time = {} millis", average_waiting_time);
    println!(":> Usage count of workers: {:?}", worker_usage_count);
    println!();
}

async fn perform_ping_test(dbs: &Vec<Database>) -> Result<Vec<time::Duration>> {
    println!(":> Performing ping test...");
    let (pings_1, pings_2) = try_join!(ping_multiple(&dbs[0].client), ping_multiple(&dbs[1].client))?;
    println!("Client 1 pings = {:?}ms", pings_1.into_iter().map(|v| v.as_millis()).collect::<Vec<_>>());
    println!("Client 2 pings = {:?}ms", pings_2.into_iter().map(|v| v.as_millis()).collect::<Vec<_>>());
    println!();
    Ok(Vec::new())
}
// ============================================================================
// ============================================================================
// ============================================================================
#[tokio::main]
async fn main() -> Result<()> {
    let hyperparameters = get_hyperparameters().await?;
    let parameters = get_parameters();

    describe_simulation_hyperparameters(&hyperparameters);
    // perform_ping_test(&hyperparameters.dbs).await?;

    let output = simulate(parameters, hyperparameters).await?;

    describe_simulation_output(&output);

    Ok(())
}
