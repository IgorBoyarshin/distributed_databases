#![allow(dead_code)]
#![allow(unused_imports)]
#![feature(destructuring_assignment)]
#![feature(duration_zero)]
#![feature(duration_saturating_ops)]
#![feature(drain_filter)]

// XXX: needed??
#![feature(linked_list_remove)]


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
use std::collections::LinkedList;

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
    user_id: UserId,
    project_id: ProjectId,
    created_at: time::Instant,
    received_at: Option<time::Instant>,
    assigned_at: Option<time::Instant>,
    finished_at: Option<time::Instant>,
    processed_at_worker: Option<usize>,
    ping_lasted: Option<time::Duration>,
    id: usize,
    // from: Location,
    // operation: Operation,
    // time: Time,
}

// struct ProcessedUserRequest {}

impl UserRequest {
    fn new(user_id: UserId, project_id: ProjectId, created_at: time::Instant, id: usize) -> UserRequest {
        UserRequest {
            user_id, project_id, created_at,
            received_at: None,
            assigned_at: None,
            finished_at: None,
            processed_at_worker: None,
            ping_lasted: None,
            id,
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
struct Library {
    dbs_for_user: HashMap<UserId, Vec<usize>>,
}

impl Library {
    fn new() -> Library {
        Library {
            dbs_for_user: HashMap::new(),
        }
    }

    fn user_registered(&self, user_id: UserId) -> bool {
        self.dbs_for_user.contains_key(&user_id)
    }

    fn register_new_user(&mut self, user_id: UserId, db_id: usize) {
        self.dbs_for_user.insert(user_id, vec![db_id]);
    }

    fn spread_user(&mut self, user_id: UserId, db_id: usize) {
        self.dbs_for_user.get_mut(&user_id).expect("invalid ID").push(db_id); // TODO: check for uniqueness
    }
}

// struct DatabaseStat {
//
// }

struct SimulationHyperParameters {
    request_amount: usize,
    input_intensity: Option<f32>,
    dbs: Vec<Database>,
    project_names: Vec<&'static str>,
    users: Vec<User>,
    synchronize_db_changes: bool,
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
        SimulationHyperParameters{ input_intensity, request_amount, mut users,
            project_names: _, dbs, synchronize_db_changes: _ }: SimulationHyperParameters) -> Result<SimulationOutput> {
    println!(":> Preparing simulation");
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
            let request = UserRequest::new(user.id, project_id, time::Instant::now(), time);
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
    let mut workers = vec![None; dbs.len()]; // all are available at the beginning
    let mut processed_user_requests = Vec::new();
    type WorkerResult = (usize, time::Instant, time::Duration);
    let (counter_tx, counter_rx) = channel::<WorkerResult>();
    let mut library = Library::new();
    let mut requests_count_of_worker = vec![0u32; dbs.len()];
    crossbeam_utils::thread::scope(|scope| {
        let mut collect_result = |workers: &mut Vec<Option<UserRequest>>, (finished_worker, finished_at, ping_lasted)| {
            let worker: &mut Option<UserRequest> = &mut workers[finished_worker];
            assert!(worker.is_some()); // must be not available yet. WTF with type???
            let mut user_request = worker.take().unwrap();
            user_request.finished_at = Some(finished_at);
            user_request.ping_lasted = Some(ping_lasted);
            processed_user_requests.push(user_request);
        };

        // ====================================================================
        type Delta = i128;
        struct WaitingStat {
            i: usize,
            count: usize,
            last_change_at: usize,
            last_waiting_time: Time,
            deltas: Vec<Delta>,
        };
        impl WaitingStat {
            fn put(&mut self, delta: Delta) {
                self.deltas[self.i] = delta;
                self.i = (self.i + 1) % self.deltas.len();
                self.count += 1;
            }

            fn mark_change(&mut self) {
                self.last_change_at = self.count;
            }

            fn new() -> WaitingStat {
                WaitingStat{ i: 0, count: 0, last_change_at: 0, last_waiting_time: 0, deltas: vec![0; 6] } // @hyper
            }
        }

        let mut waiting_stat_for_user = HashMap::new();
        // ====================================================================
        let worker_with_least_worktime = |requests_count: &Vec<u32>, dbs: &Vec<Database>| {
            // worktime = processed_count / processing_intensity
            requests_count.iter().enumerate()
                .map(|(i, &count)| (i, count as Time * dbs[i].ping_millis))
                .min_by(|(_, wt1), (_, wt2)| wt1.cmp(wt2)).expect("empty iterator")
                .0
        };
        let unclaimed_worker_with_least_worktime = |requests_count: &Vec<u32>, dbs: &Vec<Database>, able_worker_ids: &Vec<usize>| {
            requests_count.iter().enumerate()
                .filter(|(i, _)| !able_worker_ids.contains(i)) // only interested in yet unclaimed Workers
                .map(|(i, &count)| (i, count as Time * dbs[i].ping_millis))
                .min_by(|(_, wt1), (_, wt2)| wt1.cmp(wt2))
                .map(|(i, _)| i)
        };
        // ====================================================================
        // The front has most priority as it has most waiting time
        let mut queue = LinkedList::new();

        let process_received_user_request = |mut user_request: UserRequest, library: &mut Library,
                requests_count: &Vec<u32>, dbs: &Vec<Database>, waiting_stat: &mut HashMap<_, _>| {
            user_request.received_at = Some(time::Instant::now());
            if input_intensity.is_none() {
                // Means we want to test the maximum system throughput, thus it
                // makes sense to (re)set the creation_time as the reception_time (now),
                // even though *technically* they were all created almost simultaniously
                // a long time ago.
                // In this scenario, the processing intensity of the system should
                // be equal both when calculating the actual value (processed amount
                // divided by time taken) and when calculating the theoretical value
                // (using average request processing time).
                user_request.created_at = time::Instant::now();
            }
            // ================================================================
            // A new User?
            if !library.user_registered(user_request.user_id) {
                let db_id = worker_with_least_worktime(&requests_count, &dbs);
                library.register_new_user(user_request.user_id, db_id);
                waiting_stat.insert(user_request.user_id, WaitingStat::new());
                println!("Registering user {} to {}", user_request.user_id, db_id);
            }
            // ================================================================
            user_request
        };

        let mut iteration = 0;
        let mut exit_condition = None; // exit on (iteration == Some(sent_requests_count).unwrap())
        let mut processed_count = 0;
        let mut received_count = 0;
        'main: loop {
            if let Some(sent_count) = exit_condition {
                if sent_count == processed_count {
                    break 'main;
                }
            } else {
                // Collect all pending UserRequests, do not block
                while let Ok(user_request) = spawner_rx.try_recv() {
                    if let Some(user_request) = user_request {
                        // println!("Saw pending request {} from {}", user_request.id, user_request.user_id);
                        queue.push_back(process_received_user_request(user_request, &mut library,
                                &requests_count_of_worker, &dbs, &mut waiting_stat_for_user));
                        received_count += 1;
                    } else {
                        exit_condition = Some(received_count);
                    }
                }
                if queue.is_empty() {
                    // Must wait for at least one request to work with, so block
                    if let Some(user_request) = spawner_rx.recv().expect("dead spawner_tx channel") {
                        // println!("Forcefully waited for request {} from {} because queue is empty", user_request.id, user_request.user_id);
                        queue.push_back(process_received_user_request(user_request, &mut library,
                                &requests_count_of_worker, &dbs, &mut waiting_stat_for_user));
                        received_count += 1;
                    } else {
                        exit_condition = Some(received_count);
                    }
                }
            }

            // Collect finished workers, do not block
            while let Ok(result) = counter_rx.try_recv() {
                // TODO: inline function call
                collect_result(&mut workers, result);
                processed_count += 1;
            }


            // let gonna = queue.iter().any(|user_request| {
            //     let able_worker_ids = &library.dbs_for_user[&user_request.user_id];
            //     workers.iter().enumerate()
            //         .any(|(i, req)| req.is_none() && able_worker_ids.contains(&i))
            // });
            // if gonna {
            //     println!("Available workers = {:?}", workers.iter().enumerate().filter(|(_,w)| w.is_none()).map(|(i,_)| i).collect::<Vec<_>>());
            //     print!("There are {} requests in queue:", queue.len());
            //     for UserRequest{id, user_id, ..} in queue.iter() {
            //         print!("[{} from {}], ", id, user_id);
            //     }
            //     println!();
            // }

            // For each UserRequest...
            // let len_before = queue.len();
            // let user_request = queue.drain_filter(|user_request| {
            //     let able_worker_ids = &library.dbs_for_user[&user_request.user_id];
            //     workers.iter().enumerate()
            //         .any(|(i, req)| req.is_none() && able_worker_ids.contains(&i))
            // }).next();
            // if user_request.is_some() {
            //     assert_eq!(queue.len(), len_before - 1);
            // } else {
            //     assert_eq!(queue.len(), len_before);
            // }

            let task = queue.iter()
                // ... get its able_worker_ids ...
                .map(|r| &library.dbs_for_user[&r.user_id])
                // ... try to find a fit Worker for it with most priority ...
                // (a Worker is fit if free && current UserRequest can be processed on it)
                .map(|able_worker_ids|
                    workers.iter().enumerate()
                        .filter(|(i, req)| req.is_none() && able_worker_ids.contains(i))
                        .next() // get first (most priority)
                        .map(|(i, _)| i))
                .enumerate()
                // ... interested in UserRequests that currently have a fit Worker ...
                .filter(|(_, worker_id_opt)| worker_id_opt.is_some())
                // ... select first (longest in queue, most waiting time)
                .next()
                .map(|(i, w)| (i, w.unwrap())); // know it is Option::Some
            if let Some((user_request_i, chosen_worker)) = task {
                let mut user_request = queue.remove(user_request_i);
            // if let Some(mut user_request) = user_request {
                // println!("Choosing request {} from {}", user_request.id, user_request.user_id);
                println!("Iteration [{}]", iteration);
                iteration += 1;

                // let able_worker_ids = &library.dbs_for_user[&user_request.user_id];
                // let chosen_worker = workers.iter().enumerate()
                //         .filter(|(i, req)| req.is_none() && able_worker_ids.contains(i))
                //         .next() // get first (most priority)
                //         .unwrap() // know for sure there IS such a worker because we picked the user_request so
                //         .0; // need just the index
                // ================================================================
                // println!("Choosing worker {}", chosen_worker);
                user_request.assigned_at = Some(time::Instant::now());
                user_request.processed_at_worker = Some(chosen_worker);
                requests_count_of_worker[chosen_worker] += 1;
                // ================================================================
                let stat: &mut WaitingStat = waiting_stat_for_user.get_mut(&user_request.user_id).expect("unregistered user");
                let waiting_time = user_request.assigned_at.unwrap().duration_since(user_request.created_at).as_millis();
                let delta = waiting_time as Delta - stat.last_waiting_time as Delta;
                stat.last_waiting_time = waiting_time;
                stat.put(delta);

                let user_id = user_request.user_id;
                workers[chosen_worker] = Some(user_request);
                let total_delta: Delta = stat.deltas.iter().sum();
                let purify = stat.deltas.iter().all(|&d| d > 0);
                if purify && total_delta as f32 > 0.0 && stat.count > stat.last_change_at + stat.deltas.len() / 2 {
                    // println!("Because total delta {} > 0", total_delta);
                    let able_worker_ids = &library.dbs_for_user[&user_id];
                    if let Some(new_db_id) = unclaimed_worker_with_least_worktime(&requests_count_of_worker, &dbs, &able_worker_ids) {
                        stat.mark_change();
                        println!("Spreading user {} to {}", user_id, new_db_id);
                        library.spread_user(user_id, new_db_id);
                    } else {
                        println!("Nowhere to spread {}", user_id);
                    }
                }
                // ================================================================
                let inner_counter_tx = counter_tx.clone();
                let client = &dbs[chosen_worker].client;
                let _t = scope.spawn(move |_| {
                    // Process here
                    let ping_lasted = block_on(ping(&client)).expect("failed ping");

                    let finish = time::Instant::now();
                    // Report that this worker has finished and is free now
                    inner_counter_tx.send((chosen_worker, finish, ping_lasted)).expect("broken channel");
                });
                // ================================================================
            }


            // for user_request in queue.iter() {
            //     let able_worker_ids = &library.dbs_for_user[&user_request.user_id];
            //     let worker_opt = workers.iter().enumerate()
            //         .filter(|(i, req)| req.is_none() && able_worker_ids.contains(i))
            //         .next() // get first
            //         .map(|(i, _)| i);
            //     if let Some(chosen_worker) = worker_opt {
                    // // ================================================================
                    // // println!("Choosing worker {}", chosen_worker);
                    // user_request.assigned_at = Some(time::Instant::now());
                    // user_request.processed_at_worker = Some(chosen_worker);
                    // requests_count_of_worker[chosen_worker] += 1;
                    // // ================================================================
                    // let stat: &mut WaitingStat = waiting_stat_for_user.get_mut(&user_request.user_id).expect("unregistered user");
                    // let waiting_time = user_request.assigned_at.unwrap().duration_since(user_request.created_at).as_millis();
                    // let delta = waiting_time as Delta - stat.last_waiting_time as Delta;
                    // stat.last_waiting_time = waiting_time;
                    // stat.put(delta);
                    //
                    // let user_id = user_request.user_id;
                    // workers[chosen_worker] = Some(user_request);
                    // let total_delta: Delta = stat.deltas.iter().sum();
                    // let purify = stat.deltas.iter().all(|&d| d > 0);
                    // if purify && total_delta as f32 > 0.0 && stat.count > stat.last_change_at + stat.deltas.len() / 2 {
                    //     // println!("Because total delta {} > 0", total_delta);
                    //     if let Some(new_db_id) = unclaimed_worker_with_least_worktime(&requests_count_of_worker, &dbs, &able_worker_ids) {
                    //         stat.mark_change();
                    //         println!("Spreading user {} to {}", user_id, new_db_id);
                    //         library.spread_user(user_id, new_db_id);
                    //     } else {
                    //         println!("Nowhere to spread {}", user_id);
                    //     }
                    // }
                    // // ================================================================
                    // let inner_counter_tx = counter_tx.clone();
                    // let client = &dbs[chosen_worker].client;
                    // let _t = scope.spawn(move |_| {
                    //     // Process here
                    //     let ping_lasted = block_on(ping(&client)).expect("failed ping");
                    //
                    //     let finish = time::Instant::now();
                    //     // Report that this worker has finished and is free now
                    //     inner_counter_tx.send((chosen_worker, finish, ping_lasted)).expect("broken channel");
                    // });
                    // // ================================================================
            //     } // else keep searching down the queue
            // }
        }

        // XXX: collect remaining!!!
        // XXX: collect remaining!!!
        // XXX: collect remaining!!!

        // ====================================================================
        // Collect the remaining UserRequests still being processed in Workers
        // let remaining_amount = workers.iter().filter(|&r| r.is_some()).count();
        // println!("Remaining count = {}", remaining_amount);
        // for _ in 0..remaining_amount {
        //     println!("Iteration [{}]", iteration);
        //     iteration += 1;
        //
        //     let result = counter_rx.recv().expect("counter rx logic failure");
        //     collect_result(&mut workers, result);
        // }
        // println!(); // iteration print reset


        // // ====================================================================
        // // Receive incoming UserRequest
        // let mut iteration = 0;
        // while let Some(mut user_request) = spawner_rx.recv().expect("dead spawner_tx channel") {
        //     print!("\rIteration [{}]", iteration);
        //     iteration += 1;
        //
        //     let start = time::Instant::now();
        //     user_request.received_at = Some(start);
        //     if input_intensity.is_none() {
        //         // Means we want to test the maximum system throughput, thus it
        //         // makes sense to (re)set the creation_time as the reception_time (now),
        //         // even though *technically* they were all created almost simultaniously
        //         // a long time ago.
        //         // In this scenario, the processing intensity of the system should
        //         // be equal both when calculating the actual value (processed amount
        //         // divided by time taken) and when calculating the theoretical value
        //         // (using average request processing time).
        //         user_request.created_at = start;
        //     }
        //
        //     // ================================================================
        //     // A new User?
        //     if !library.user_registered(user_request.user_id) {
        //         let db_id = worker_with_least_worktime(&requests_count_of_worker, &dbs);
        //         library.register_new_user(user_request.user_id, db_id);
        //         waiting_stat_for_user.insert(user_request.user_id, WaitingStat::new());
        //     }
        //     // ================================================================
        //     // Workers that contain this User's data, so only these Workers can process this request
        //     let able_worker_ids = &library.dbs_for_user[&user_request.user_id];
        //     let worker_opt = workers.iter().enumerate()
        //         .filter(|(i, req)| req.is_none() && able_worker_ids.contains(i))
        //         .next()
        //         .map(|(i, _)| i);
        //     let chosen_worker = if let Some(worker) = worker_opt { worker } else {
        //         loop {
        //             // println!("Waiting for the proper worker to show up...");
        //             let result = counter_rx.recv().expect("counter rx logic failure");
        //             let finished_worker = result.0;
        //             collect_result(&mut workers, result);
        //             if able_worker_ids.contains(&finished_worker) {
        //                 break finished_worker;
        //             }
        //         }
        //     };
        //     // Try to collect others but do not block
        //     while let Ok(result) = counter_rx.try_recv() {
        //         collect_result(&mut workers, result);
        //     }
        //     // ================================================================
        //     // println!("Choosing worker {}", chosen_worker);
        //     user_request.assigned_at = Some(time::Instant::now());
        //     user_request.processed_at_worker = Some(chosen_worker);
        //     requests_count_of_worker[chosen_worker] += 1;
        //     // ================================================================
        //     let stat: &mut WaitingStat = waiting_stat_for_user.get_mut(&user_request.user_id).expect("unregistered user");
        //     let waiting_time = user_request.assigned_at.unwrap().duration_since(user_request.created_at).as_millis();
        //     let delta = waiting_time as Delta - stat.last_waiting_time as Delta;
        //     stat.last_waiting_time = waiting_time;
        //     stat.put(delta);
        //
        //     let user_id = user_request.user_id;
        //     workers[chosen_worker] = Some(user_request);
        //     let total_delta: Delta = stat.deltas.iter().sum();
        //     let purify = stat.deltas.iter().all(|&d| d > 0);
        //     if purify && total_delta as f32 > 0.0 && stat.count > stat.last_change_at + stat.deltas.len() / 2 {
        //         // println!("Because total delta {} > 0", total_delta);
        //         if let Some(new_db_id) = unclaimed_worker_with_least_worktime(&requests_count_of_worker, &dbs, &able_worker_ids) {
        //             stat.mark_change();
        //             println!("Spreading user {} to {}", user_id, new_db_id);
        //             library.spread_user(user_id, new_db_id);
        //         } else {
        //             println!("Nowhere to spread {}", user_id);
        //         }
        //     }
        //     // ================================================================
        //     let inner_counter_tx = counter_tx.clone();
        //     let client = &dbs[chosen_worker].client;
        //     let _t = scope.spawn(move |_| {
        //         // Process here
        //         let ping_lasted = block_on(ping(&client)).expect("failed ping");
        //
        //         let finish = time::Instant::now();
        //         // Report that this worker has finished and is free now
        //         inner_counter_tx.send((chosen_worker, finish, ping_lasted)).expect("broken channel");
        //     });
        //     // ================================================================
        // }
        // // println!(); // iteration print reset

        // // ====================================================================
        // // Collect the remaining ones still trapped in the system
        // let remaining_amount = workers.iter().filter(|&r| r.is_some()).count();
        // for _ in 0..remaining_amount {
        //     print!("\rIteration [{}]", iteration);
        //     iteration += 1;
        //
        //     let result = counter_rx.recv().expect("counter rx logic failure");
        //     collect_result(&mut workers, result);
        // }
        // println!(); // iteration print reset
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
    ping_millis: Time,
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
    let mut dbs = vec![
        Database {
            client: Client::with_uri_str(env::var("MONGO_CHRISTMAS").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
            name: "Christmas Tree",
            ping_millis: 0,
        },
        Database {
            client: Client::with_uri_str(env::var("MONGO_ORANGE").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
            name: "Orange Tree",
            ping_millis: 0,
        },
        Database {
            client: Client::with_uri_str(env::var("MONGO_LEMON").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
            name: "Lemon Tree",
            ping_millis: 0,
        },
        Database {
            client: Client::with_uri_str(env::var("MONGO_MAPLE").expect("Set the MONGO_<NAME> env!").as_ref()).await?,
            name: "Maple Tree",
            ping_millis: 0,
        },
    ];
    let project_names = vec!["Quartz", "Pyrite", "Lapis Lazuli", "Amethyst", "Jasper", "Malachite", "Diamond"];

    let projects_per_user = 4;
    let projects_count = project_names.len();
    // Users are created here and not inside simulation based on hyperparameters
    // because there is an element of random in creation (e.g. Behavior), and
    // we would like all simulation to be conducted with the same set of users.
    let users = User::create_users(5, projects_count, projects_per_user);


    println!(":> Determining ping to DBs...");
    // Parallel ping seems to give skewed results. As this procedure is not that
    // long and is done only once, we don't mind waiting a bit for sequential ping.
    for db in dbs.iter_mut() {
        db.ping_millis = determine_ping(&db.client).await?.as_millis();
    }

    println!(":> Sorting DBs based on ping");
    dbs.sort_by(|Database{ ping_millis: p1, ..}, Database{ ping_millis: p2, ..}| p1.cmp(p2));
    print!("Will use such order: ");
    for db in dbs.iter() {
        print!("{}({}ms); ", db.name, db.ping_millis);
    }
    println!();

    let max_processing_intensity = dbs.iter()
        .map(|db| db.ping_millis)
        .fold(0.0, |acc, x| acc + 1000.0 / (x as f32));
    println!(":> Max processing intensity of the system is {} requests per second ({} millis per request)",
        max_processing_intensity, (1000.0 / max_processing_intensity) as u32);

    Ok(SimulationHyperParameters {
        request_amount: 64,
        // input_intensity: None,
        input_intensity: Some(0.7 * max_processing_intensity),
        // input_intensity: Some(1.05 * processing_intensity),
        dbs,
        project_names,
        users,
        synchronize_db_changes: false,
    })
}

fn get_parameters() -> SimulationParameters {
    SimulationParameters {
        spread_rate: 1.0,
        decay_rate: 3.0,
    }
}

fn describe_simulation_hyperparameters(SimulationHyperParameters{ users: _, .. }: &SimulationHyperParameters) {
    // println!(":> Users:");
    // for user in users.iter() { describe_user(&user); }
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
    for UserRequest{ created_at, received_at, assigned_at, finished_at, processed_at_worker, ping_lasted, id, user_id, .. } in processed_user_requests {
        let received_at = received_at.expect("empty time Option while describing processed request");
        let assigned_at = assigned_at.expect("empty time Option while describing processed request");
        let finished_at = finished_at.expect("empty time Option while describing processed request");
        let processed_at_worker = processed_at_worker.expect("empty num Option while describing processed request");
        let ping_lasted = ping_lasted.expect("empty ping Option while describing processed request");

        let waiting_time = assigned_at.duration_since(*created_at).as_millis();
        let total_time = finished_at.duration_since(received_at).as_millis();
        average_total_time += total_time;
        average_waiting_time += waiting_time;
        println!("Request [{:>4}] from {:>5} waited for {:>7} millis, processed in {:>8} millis, processed at {}, ping lasted {}ms",
            id,
            "#".repeat(*user_id as usize),
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
// ============================================================================
// ============================================================================
// ============================================================================
#[tokio::main]
async fn main() -> Result<()> {
    let hyperparameters = get_hyperparameters().await?;
    let parameters = get_parameters();

    describe_simulation_hyperparameters(&hyperparameters);

    let output = simulate(parameters, hyperparameters).await?;

    describe_simulation_output(&output);

    Ok(())
}
