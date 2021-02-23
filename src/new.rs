#![allow(dead_code)]
// #![allow(unused_imports)]
#![feature(destructuring_assignment)]
#![feature(duration_zero)]
#![feature(duration_saturating_ops)]
#![feature(linked_list_remove)]


use mongodb::{
    bson,
    bson::{doc, Bson},
    // bson::document::Document,
    // error::Result,
    Client
};
use mongodb::error::Result as MongoResult;
use std::env;
use futures_util::StreamExt;
// use futures::future::join_all;
// use futures::join;
// use futures::try_join;
// use futures::future::try_join_all;
use serde::{Deserialize, Serialize};

use std::collections::HashMap;
use std::collections::LinkedList;

use rand::distributions::Open01;
use rand_chacha::ChaCha8Rng as ChaChaRng;
use rand::{ Rng, SeedableRng };

// use std::sync::{Arc, Mutex};
// type Random = Arc<Mutex<dyn RngCore>>;


use variant_count::VariantCount;

// use std::fs::File;
// use std::io::Write;
// use plotters::prelude::{RED, WHITE, ChartBuilder, LineSeries, BitMapBackend};


use futures::executor::block_on;
use std::thread;
use std::time;
use std::sync::mpsc::channel;
// use tokio::time as tokio_time;
// ============================================================================
// ============================================================================
// ============================================================================
#[derive(VariantCount, Debug)]
enum UserBehavior {
    AveragedSequential {
        amount: u32,
        left: u32,
        project_index: usize,
    },
    AveragedRandom,
    PreferentialMono {
        project_index: usize,
    },
    PreferentialDuo {
        project_index_1: usize,
        project_index_2: usize,
    },
}

impl UserBehavior {
    fn gen_index(&mut self, random: &mut ChaChaRng, projects_amount: usize) -> usize {
        match self {
            UserBehavior::AveragedSequential{ amount, left, project_index } => {
                let max_amount = 10;

                if *left == 0 {
                    *amount     = random.gen_range(1..=max_amount);
                    *project_index = random.gen_range(0..projects_amount);
                    *left       = *amount;
                }
                *left -= 1;

                *project_index
            },
            UserBehavior::AveragedRandom => {
                random.gen_range(0..projects_amount)
            },
            UserBehavior::PreferentialMono{ project_index } =>{
                let preferential_probability = 0.5;
                let use_preferred = random.sample::<f32, _>(Open01) < preferential_probability;
                if use_preferred {
                    *project_index
                } else {
                    random.gen_range(0..projects_amount)
                }
            },
            UserBehavior::PreferentialDuo{ project_index_1, project_index_2 } =>{
                let preferential_probability = 0.5 / 2.0;
                let use_preferred_chance = random.sample::<f32, _>(Open01);
                if use_preferred_chance <= preferential_probability {
                    *project_index_1
                } else if use_preferred_chance <= 2.0 * preferential_probability {
                    *project_index_2
                } else {
                    random.gen_range(0..projects_amount)
                }
            },
        }
    }

    fn random(random: &mut ChaChaRng, projects_amount: usize) -> UserBehavior {
        match random.gen_range(0..UserBehavior::VARIANT_COUNT) {
            0 => UserBehavior::AveragedSequential{ amount: /* any */ 0, left: 0, project_index: /* any */ 0 },
            1 => UserBehavior::AveragedRandom,
            2 => UserBehavior::PreferentialMono{ project_index: random.gen_range(0..projects_amount) },
            3 => {
                assert!(projects_amount > 1);
                let project_index_1 = random.gen_range(0..projects_amount);
                let project_index_2 = loop {
                    let project_index = random.gen_range(0..projects_amount);
                    if project_index != project_index_1 {
                        break project_index;
                    }
                };
                UserBehavior::PreferentialDuo{ project_index_1, project_index_2 }
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
    fn gen(&mut self, random: &mut ChaChaRng) -> ProjectId {
        self.project_ids[self.user_behavior.gen_index(random, self.project_ids.len())]
    }

    fn create_users(random: &mut ChaChaRng, users_amount: usize, projects_amount: usize, projects_per_user: usize) -> Vec<User> {
        let mut vec = Vec::with_capacity(users_amount);
        for id in 0..users_amount {
            let user_behavior = UserBehavior::random(random, projects_per_user);
            let project_ids = generate_count_random_indices_until(random, projects_per_user, projects_amount);
            vec.push(User{ user_behavior, id: id as UserId, project_ids });
        }
        vec
    }
}

fn generate_count_random_indices_until(random: &mut ChaChaRng, count: usize, len: usize) -> Vec<ProjectId> {
    let mut vec = Vec::with_capacity(count as usize);
    let mut left = count;
    for index in 0..len {
        let remaining_indices = len - index;
        let take_probability = (left as f32) / (remaining_indices as f32);
        let take = random.sample::<f32, _>(Open01) < take_probability;
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
type UserId = usize;
type Time = u128;

type IterationType = u128;

#[derive(Debug, Copy, Clone)]
enum MyTime {
    Instant(time::Instant),
    Iteration(IterationType),
}
#[derive(Debug, Copy, Clone)]
enum MyDuration {
    Timed(time::Duration),
    Iterated(IterationType),
}

impl MyTime {
    fn duration_since(&self, other: &MyTime) -> MyDuration {
        match self {
            MyTime::Instant(time) => {
                if let MyTime::Instant(other_time) = other {
                    MyDuration::Timed(time.duration_since(*other_time))
                } else { panic!("Different MyTimes used"); }
            },
            MyTime::Iteration(iteration) => {
                if let MyTime::Iteration(other_iteration) = other {
                    MyDuration::Iterated(iteration - other_iteration)
                } else { panic!("Different MyTimes used"); }
            },
        }
    }
}

impl MyDuration {
    fn as_secs(&self) -> u128 {
        match self {
            MyDuration::Timed(duration) => duration.as_secs() as u128,
            MyDuration::Iterated(nanos) => nanos / 1_000_000_000,
        }
    }

    fn as_millis(&self) -> u128 {
        match self {
            MyDuration::Timed(duration) => duration.as_millis(),
            MyDuration::Iterated(nanos) => nanos / 1_000_000,
        }
    }

    fn as_micros(&self) -> u128 {
        match self {
            MyDuration::Timed(duration) => duration.as_micros(),
            MyDuration::Iterated(nanos) => nanos / 1_000,
        }
    }
}

// #[derive(Debug, Copy, Clone, Deserialize, Serialize)]
#[derive(Debug, Copy, Clone)]
struct UserRequest {
    user_id: UserId,
    project_id: ProjectId,
    created_at: MyTime,
    received_at: Option<MyTime>,
    assigned_at: Option<MyTime>,
    finished_at: Option<MyTime>,
    processed_at_worker: Option<usize>,
    ping_lasted: Option<MyDuration>,
    triggered_spread: bool,
    processed_at_iteration: usize,
    id: usize,
    // from: Location,
    // operation: Operation,
    // time: Time,
}

// struct ProcessedUserRequest {}

impl UserRequest {
    fn new(user_id: UserId, project_id: ProjectId, created_at: MyTime, id: usize) -> UserRequest {
        UserRequest {
            user_id, project_id, created_at,
            received_at: None,
            assigned_at: None,
            finished_at: None,
            processed_at_worker: None,
            ping_lasted: None,
            triggered_spread: false,
            processed_at_iteration: 0,
            id,
        }
    }
}
// ============================================================================
// ============================================================================
// ============================================================================
async fn ping(client: &Client) -> MongoResult<time::Duration> {
    let start = time::Instant::now();
    {
        let _names = client.database("users").list_collection_names(None).await?;
        // let _s = dump_to_str(&client).await?;
    }
    Ok(start.elapsed())
}

async fn ping_multiple(client: &Client) -> MongoResult<Vec<time::Duration>> {
    let amount = 10;
    let mut times = Vec::with_capacity(amount);
    for _ in 0..amount {
        times.push(ping(&client).await?);
    }
    Ok(times)
}

async fn determine_ping(client: &Client) -> MongoResult<time::Duration> {
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
    dbs_for_project: HashMap<ProjectId, Vec<usize>>,
}

impl Library {
    fn new() -> Library {
        Library {
            dbs_for_user: HashMap::new(),
            dbs_for_project: HashMap::new(),
        }
    }

    fn project_registered(&self, project_id: ProjectId) -> bool {
        self.dbs_for_project.contains_key(&project_id)
    }

    fn register_new_project(&mut self, project_id: ProjectId, db_id: usize) {
        self.dbs_for_project.insert(project_id, vec![db_id]);
    }

    fn spread_project_to(&mut self, project_id: ProjectId, db_id: usize) {
        assert!(!self.dbs_for_project[&project_id].contains(&db_id));
        self.dbs_for_project.get_mut(&project_id).expect("invalid ID").push(db_id);
    }
}

struct SimulationHyperParameters {
    request_amount: usize,
    input_intensity: Option<f32>,
    dbs: Vec<Database>,
    project_names: Vec<String>,
    // project_names: Vec<&'static str>,
    users: Vec<User>,
    synchronize_db_changes: bool,
}

struct SimulationParameters {
    spread_rate: f32,
    decay_rate: f32,
}

struct SimulationOutput {
    start: MyTime,
    duration: MyDuration,
    processed_user_requests: Vec<UserRequest>,
    dbs_for_project: HashMap<ProjectId, Vec<usize>>,
    average_db_ping_millis: Vec<Time>,
}

type Delta = i128;
struct WaitingStat {
    i: usize,
    count: usize,
    last_change_at: usize,
    last_waiting_time: Time,
    deltas: Vec<Delta>,
}
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

// NOTE
// NOTE registration occurs on creation, data may be outdated
// NOTE
fn simulate_fake(
        mut random: ChaChaRng,
        SimulationParameters{ spread_rate: _, decay_rate: _ }: SimulationParameters,
        SimulationHyperParameters{ input_intensity, request_amount, mut users,
            project_names: _, dbs, synchronize_db_changes: _ }: SimulationHyperParameters) -> SimulationOutput {

    let mut iteration: IterationType = 0;
    let mut user_request_id_to_spawn = 0;
    // let mut spawner_queue = LinkedList::new(); // front has the oldest
    let mut queue = LinkedList::new(); // the front has most priority as it has most waiting time
    let mut spawning_done = false;

    let simulation_start = MyTime::Iteration(iteration);
    let mut processed_user_requests = Vec::new();
    let mut library = Library::new();

    let mut workers: Vec<Option<UserRequest>> = vec![None; dbs.len()]; // all are available at the beginning
    let mut requests_count_of_worker = vec![0u32; dbs.len()];
    let mut waiting_stat_for_project = HashMap::new();
    // type WorkerResult = (usize, MyTime, MyTime);
    // let mut counter_queue = LinkedList<WorkerResult>::new(); // TODO type needed??
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
    let mut processing_number = 0;

    let mut pending_workers = vec![IterationType::MAX; dbs.len()];
    let mut spawn_wake_at = 0;

    'simulation_loop: loop {
        // println!(":> Woke at iteration {} with processed count = {}", iteration, processed_user_requests.len());
        // Spawn UserRequests
        if iteration == spawn_wake_at && !spawning_done {
            // println!(":> .... to spawn some!");
            'spawning_loop: loop { // loop is used to make instant multi-spawning possible
                // Pick random user
                let len = users.len();
                let user = &mut users[random.gen_range(0..len)];

                // Pick project for this User according to his Strategy
                let project_id = user.gen(&mut random);

                // Send for execution
                let mut request = UserRequest::new(user.id, project_id, MyTime::Iteration(iteration), user_request_id_to_spawn);
                request.received_at = Some(MyTime::Iteration(iteration));
                user_request_id_to_spawn += 1;
                // A new Project?
                if !library.project_registered(request.project_id) {
                    let db_id = worker_with_least_worktime(&requests_count_of_worker, &dbs);
                    library.register_new_project(request.project_id, db_id);
                    waiting_stat_for_project.insert(request.project_id, WaitingStat::new());
                    println!("Registering project {} to {}", request.project_id, db_id);
                }
                queue.push_back(request);

                // Finish simulation?
                if user_request_id_to_spawn == request_amount {
                    spawning_done = true;
                    spawn_wake_at = IterationType::MAX;
                    break 'spawning_loop;
                }

                if let Some(input_intensity) = input_intensity {
                    // Go to sleep
                    // let sleep_duration = -random.sample::<f32, _>(Open01).ln() / input_intensity;
                    let sleep_duration = 1.0 / input_intensity;
                    let nanos_in_second = 1_000_000_000.0;
                    let sleep_nanos = (nanos_in_second * sleep_duration) as u64;
                    // println!("Sleeping for {} nanos", sleep_nanos as u64);

                    spawn_wake_at = iteration + sleep_nanos as u128;
                    break 'spawning_loop;
                }
            }
        }

        // ======================== Free workers ==============================
        for (worker_id, wake_at) in pending_workers.iter_mut().enumerate() {
            if *wake_at == iteration { // time to wake this worker
                *wake_at = IterationType::MAX;

                let worker = &mut workers[worker_id];
                assert!(worker.is_some()); // must be not available yet
                let mut user_request = worker.take().unwrap();
                user_request.finished_at = Some(MyTime::Iteration(iteration));
                processed_user_requests.push(user_request);
            }
        }


        // ======================== Put for execution =========================
        // For each UserRequest...
        let task = queue.iter() // NOTE: to make this a simple queue without look-through just .take(1)
            // ... get its able_worker_ids ...
            .map(|r| &library.dbs_for_project[&r.project_id])
            // ... try to find a fit Worker for it with most priority ...
            // (a Worker is fit if free && current UserRequest can be processed on it)
            .map(|able_worker_ids|
                workers.iter().enumerate()
                    .filter(|(i, req)| req.is_none() && able_worker_ids.contains(i))
                    .next() // get first (best performance)
                    .map(|(i, _)| i))
            .enumerate()
            // ... interested in UserRequests that currently have a fit Worker ...
            .filter(|(_, worker_id_opt)| worker_id_opt.is_some())
            // ... select first (longest in queue, most waiting time)
            .next()
            .map(|(i, w)| (i, w.unwrap())); // checked that it is Option::Some earlier
        if let Some((user_request_i, chosen_worker)) = task {
            // println!(":> .... to assign some!");
            let mut user_request = queue.remove(user_request_i);
            // println!("Processing number [{}] with queue of {}", processing_number, queue.len());
            processing_number += 1;

            // ================================================================
            // println!("Choosing worker {}", chosen_worker);
            user_request.assigned_at = Some(MyTime::Iteration(iteration));
            user_request.processed_at_worker = Some(chosen_worker);
            user_request.processed_at_iteration = processing_number - 1;
            requests_count_of_worker[chosen_worker] += 1;
            // ================================================================
            // Update WaitingStat required for making decisions
            let project_id = user_request.project_id;
            let stat: &mut WaitingStat = waiting_stat_for_project.get_mut(&project_id).expect("unregistered project");
            let waiting_time = user_request.assigned_at.unwrap().duration_since(&user_request.created_at).as_millis();
            let delta = waiting_time as Delta - stat.last_waiting_time as Delta;
            stat.last_waiting_time = waiting_time;
            stat.put(delta);

            // Make decision
            let delta_rising = stat.deltas.iter().all(|&d| d > 0);
            let last_change_long_enough_ago = stat.count > stat.last_change_at + stat.deltas.len() / 2;
            if delta_rising && last_change_long_enough_ago {
                let able_worker_ids = &library.dbs_for_project[&project_id];
                if let Some(new_db_id) = unclaimed_worker_with_least_worktime(&requests_count_of_worker, &dbs, &able_worker_ids) {
                    stat.mark_change();
                    library.spread_project_to(project_id, new_db_id);
                    user_request.triggered_spread = true;
                    println!("Spreading project {} to {}", project_id, new_db_id);
                    // println!("... because deltas are {:?}", stat.deltas);
                } else {
                    println!("Nowhere to spread {}", project_id);
                }
            }
            // println!("... because deltas are {:?}", stat.deltas);

            // ================================================================
            // Launch processing thread
            let db = &dbs[chosen_worker];
            let sleep_nanos = db.ping_millis * 1_000_000;
            user_request.ping_lasted = Some(MyDuration::Iterated(sleep_nanos));
            // Mark Worker as busy
            workers[chosen_worker] = Some(user_request);
            pending_workers[chosen_worker] = iteration + sleep_nanos;
        }

        // ======================== Finish simulation? ========================
        if spawning_done && processed_user_requests.len() == request_amount {
            break 'simulation_loop;
        }

        // ======================== Decide when to wake up ====================
        let mut min = IterationType::MAX;
        for &wake_at in pending_workers.iter() {
            if wake_at < min { min = wake_at; }
        }
        if spawn_wake_at < min { min = spawn_wake_at; }
        iteration = min;
    }
    let simulation_duration = MyTime::Iteration(iteration).duration_since(&simulation_start);
    println!();

    SimulationOutput{ start: simulation_start, duration: simulation_duration, processed_user_requests,
        dbs_for_project: library.dbs_for_project,
        average_db_ping_millis: dbs.iter().map(|db| db.ping_millis).collect::<Vec<_>>() }
}

async fn simulate(
        mut random: ChaChaRng,
        SimulationParameters{ spread_rate: _, decay_rate: _ }: SimulationParameters,
        SimulationHyperParameters{ input_intensity, request_amount, mut users,
            project_names: _, dbs, synchronize_db_changes: _ }: SimulationHyperParameters) -> MongoResult<SimulationOutput> {

    println!(":> Preparing simulation");
    let (spawner_tx, spawner_rx) = channel();


    // Responsible for spawning UserRequests
    thread::spawn(move|| {
        let mut time = 0;
        loop {
            // Pick random user
            let len = users.len();
            let user = &mut users[random.gen_range(0..len)];

            // Pick project for this User according to his Strategy
            let project_id = user.gen(&mut random);

            // Send for execution
            let request = UserRequest::new(user.id, project_id, MyTime::Instant(time::Instant::now()), time);
            spawner_tx.send(Some(request)).unwrap();

            if let Some(input_intensity) = input_intensity {
                // Go to sleep
                // let sleep_duration_millis = -millis_in_second * rand::thread_rng().sample::<f32, _>(Open01).ln() / input_intensity;
                let sleep_duration = -random.sample::<f32, _>(Open01).ln() / input_intensity;
                // let millis_in_second = 1_000.0;
                // let sleep_millis = (millis_in_second * sleep_duration) as u64;
                // let duration = time::Duration::from_millis(sleep_millis);
                // println!("Sleeping for {} millis", sleep_millis as u64);
                let micros_in_second = 1_000_000.0;
                let sleep_micros = (micros_in_second * sleep_duration) as u64;
                let duration = time::Duration::from_micros(sleep_micros);
                // println!("Sleeping for {} micros", sleep_micros as u64);

                thread::sleep(duration);
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
    let mut processed_user_requests = Vec::new();
    let mut library = Library::new();
    crossbeam_utils::thread::scope(|scope| {
        let mut workers: Vec<Option<UserRequest>> = vec![None; dbs.len()]; // all are available at the beginning
        let mut requests_count_of_worker = vec![0u32; dbs.len()];
        let mut waiting_stat_for_project = HashMap::new();
        type WorkerResult = (usize, MyTime, MyDuration);
        let (counter_tx, counter_rx) = channel::<WorkerResult>();
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
        let process_received_user_request = |mut user_request: UserRequest, library: &mut Library,
                requests_count: &Vec<u32>, dbs: &Vec<Database>, waiting_stat: &mut HashMap<_, _>| {
            user_request.received_at = Some(MyTime::Instant(time::Instant::now()));
            if input_intensity.is_none() {
                // Means we want to test the maximum system throughput, thus it
                // makes sense to (re)set the creation_time as the reception_time (now),
                // even though *technically* they were all created almost simultaniously
                // a long time ago.
                // In this scenario, the processing intensity of the system should
                // be equal both when calculating the actual value (processed amount
                // divided by time taken) and when calculating the theoretical value
                // (using average request processing time).
                user_request.created_at = MyTime::Instant(time::Instant::now());
            }
            // ================================================================
            // A new Project?
            if !library.project_registered(user_request.project_id) {
                let db_id = worker_with_least_worktime(&requests_count, &dbs);
                library.register_new_project(user_request.project_id, db_id);
                waiting_stat.insert(user_request.project_id, WaitingStat::new());
                println!("Registering project {} to {}", user_request.project_id, db_id);
            }
            // ================================================================
            user_request
        };
        // ====================================================================
        let mut queue = LinkedList::new(); // the front has most priority as it has most waiting time
        let mut iteration = 0;
        let mut exit_condition = None; // exit on (iteration == Some(sent_requests_count).unwrap())
        let mut received_count = 0;

        let mut threads = Vec::with_capacity(dbs.len());
        let mut threads_tx = Vec::with_capacity(dbs.len());
        for _ in 0..dbs.len() {
            println!(":> Spawning Worker thread...");
            let (tx, rx) = channel::<Option<(std::sync::mpsc::Sender<WorkerResult>, &Database, usize)>>();

            let t = scope.spawn(move |_| {
                while let Some((counter_tx, db, chosen_worker)) = rx.recv().expect("broken thread pipe rx") {
                    // Process here
                    let ping_lasted = MyDuration::Timed(if let Some(client) = &db.client {
                        block_on(ping(&client)).expect("failed ping")
                    } else {
                        let duration = time::Duration::from_millis(db.ping_millis as u64);
                        crossbeam::channel::after(duration).recv().unwrap();
                        duration
                    });

                    let finish = MyTime::Instant(time::Instant::now());
                    // Report that this Worker has finished and is free now
                    let worker_result = (chosen_worker, finish, ping_lasted);
                    counter_tx.send(worker_result).expect("broken channel");
                }

                // Received None => time to die...
                println!(":> Terminating Worker thread...");
            });

            threads.push(t);
            threads_tx.push(tx);
        }

        'main: loop {
            if let Some(sent_count) = exit_condition {
                if sent_count == processed_user_requests.len() {
                    // We have processed everything that was sent to us
                    break 'main;
                }
            } else {
                // Collect all pending UserRequests, do not block
                while let Ok(user_request) = spawner_rx.try_recv() {
                    if let Some(user_request) = user_request {
                        queue.push_back(process_received_user_request(user_request, &mut library,
                                &requests_count_of_worker, &dbs, &mut waiting_stat_for_project));
                        received_count += 1;
                    } else {
                        exit_condition = Some(received_count);
                    }
                }
                if queue.is_empty() && exit_condition.is_none() {
                    // Must wait for at least one request to work with, so block
                    if let Some(user_request) = spawner_rx.recv().expect("dead spawner_tx channel") {
                        queue.push_back(process_received_user_request(user_request, &mut library,
                                &requests_count_of_worker, &dbs, &mut waiting_stat_for_project));
                        received_count += 1;
                    } else {
                        exit_condition = Some(received_count);
                    }
                }
            }

            // Collect finished workers, do not block
            while let Ok((finished_worker, finished_at, ping_lasted)) = counter_rx.try_recv() {
                let worker = &mut workers[finished_worker];
                assert!(worker.is_some()); // must be not available yet
                let mut user_request = worker.take().unwrap();
                user_request.finished_at = Some(finished_at);
                user_request.ping_lasted = Some(ping_lasted);
                processed_user_requests.push(user_request);
            }

            // For each UserRequest...
            let task = queue.iter() // NOTE: to make this a simple queue without look-through just .take(1)
                // ... get its able_worker_ids ...
                .map(|r| &library.dbs_for_project[&r.project_id])
                // ... try to find a fit Worker for it with most priority ...
                // (a Worker is fit if free && current UserRequest can be processed on it)
                .map(|able_worker_ids|
                    workers.iter().enumerate()
                        .filter(|(i, req)| req.is_none() && able_worker_ids.contains(i))
                        .next() // get first (best performance)
                        .map(|(i, _)| i))
                .enumerate()
                // ... interested in UserRequests that currently have a fit Worker ...
                .filter(|(_, worker_id_opt)| worker_id_opt.is_some())
                // ... select first (longest in queue, most waiting time)
                .next()
                .map(|(i, w)| (i, w.unwrap())); // checked that it is Option::Some earlier
            if let Some((user_request_i, chosen_worker)) = task {
                let mut user_request = queue.remove(user_request_i);
                println!("Iteration [{}] with queue of {}", iteration, queue.len());
                // print!("\rIteration [{}]", iteration);
                iteration += 1;

                // ================================================================
                // println!("Choosing worker {}", chosen_worker);
                user_request.assigned_at = Some(MyTime::Instant(time::Instant::now()));
                user_request.processed_at_worker = Some(chosen_worker);
                user_request.processed_at_iteration = iteration - 1;
                requests_count_of_worker[chosen_worker] += 1;
                // ================================================================
                // Update WaitingStat required for making decisions
                let project_id = user_request.project_id;
                let stat: &mut WaitingStat = waiting_stat_for_project.get_mut(&project_id).expect("Unregistered project");
                let waiting_time = user_request.assigned_at.unwrap().duration_since(&user_request.created_at).as_millis();
                let delta = waiting_time as Delta - stat.last_waiting_time as Delta;
                stat.last_waiting_time = waiting_time;
                stat.put(delta);

                // Make decision
                let delta_rising = stat.deltas.iter().all(|&d| d > 0);
                let last_change_long_enough_ago = stat.count > stat.last_change_at + stat.deltas.len() / 2;
                if delta_rising && last_change_long_enough_ago {
                    let able_worker_ids = &library.dbs_for_project[&project_id];
                    if let Some(new_db_id) = unclaimed_worker_with_least_worktime(&requests_count_of_worker, &dbs, &able_worker_ids) {
                        stat.mark_change();
                        library.spread_project_to(project_id, new_db_id);
                        user_request.triggered_spread = true;
                        println!("Spreading project {} to {}", project_id, new_db_id);
                    } else {
                        println!("Nowhere to spread {}", project_id);
                    }
                }

                // Mark Worker as busy
                workers[chosen_worker] = Some(user_request);
                // ================================================================
                // Launch processing thread
                let inner_counter_tx = counter_tx.clone();
                let db = &dbs[chosen_worker];
                threads_tx[chosen_worker].send(Some((inner_counter_tx, db, chosen_worker))).expect("broken thread pipe tx");
            }
        }

        // Finish all threads
        for tx in threads_tx.into_iter() {
            tx.send(None).expect("broken thread pipe tx");
        }
        for thread in threads.into_iter() {
            thread.join().expect("unable to join Worker");
        }
    }).expect("crossbeam scope unwrap failure");

    let simulation_duration = simulation_start.elapsed();
    println!();

    Ok(SimulationOutput{ start: MyTime::Instant(simulation_start), duration: MyDuration::Timed(simulation_duration), processed_user_requests,
        dbs_for_project: library.dbs_for_project,
        average_db_ping_millis: dbs.iter().map(|db| db.ping_millis).collect::<Vec<_>>() })
}
// ============================================================================
// ============================================================================
// ============================================================================
struct Database {
    client: Option<Client>,
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

async fn dump_to_str(client: &Client) -> MongoResult<String> {
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
async fn get_client(env_name: &str) -> MongoResult<Client> {
    Client::with_uri_str(env::var(env_name).expect("Set the MONGO_<NAME> env!").as_ref()).await
}

async fn get_hyperparameters(random: &mut ChaChaRng, is_real: bool) -> MongoResult<SimulationHyperParameters> {
    let mut dbs: Vec<Database> = if is_real {
        println!(":> Performing simulation with real MondoDBs");
        println!(":> Determining ping to DBs...");
        // Parallel ping seems to give skewed results. As this procedure is not that
        // long and is done only once, we don't mind waiting a bit for sequential ping.

        vec![
            ("MONGO_CHRISTMAS", "Christmas Tree"),
            ("MONGO_ORANGE", "Orange Tree"),
            ("MONGO_LEMON", "Lemon Tree"),
            ("MONGO_MAPLE", "Maple Tree"),
        ].into_iter()
        .map(|(env,    name)| (block_on(get_client(env)).expect("failed to get client"), name))
        .map(|(client, name)| Database {
            ping_millis: block_on(determine_ping(&client)).expect("ping failed").as_millis(),
            client: Some(client),
            name: name
        })
        .collect()
    } else {
        println!(":> Performing fake simulation");
        vec![
            // XXX
            // XXX Increasing the time leads to improved accuracy.
            // XXX Numbers starting from 1000 up should be sufficient.
            // XXX
            (1_000 * 262, "Christmas Tree"), // 262
            (1_000 * 71, "Orange Tree"), // 71
            (1_000 * 131, "Lemon Tree"), // 131
            (1_000 * 41, "Maple Tree"), // 41

            (1_000 * 31, "Maple Tree 2"), // 41
            (1_000 * 121, "Maple Tree 3"), // 41
            (1_000 * 95, "Maple Tree 4"), // 41
            // (1_000 * 91, "Maple Tree 5"), // 41
            // (1_000 * 141, "Maple Tree 6"), // 41
            // (1_000 * 61, "Christmas Tree"), // 262
            // (1_000 * 13, "Orange Tree"), // 71
            // (1_000 * 23, "Lemon Tree"), // 131
            // (1_000 * 7, "Maple Tree") // 41
        ].into_iter()
        .map(|(ping, name)| Database { client: None, name: name, ping_millis: ping })
        .collect()
    };

    // let project_names = vec!["Quartz", "Pyrite", "Lapis Lazuli", "Amethyst", "Jasper", "Malachite", "Diamond"];
    let project_names = (0..20).map(|i| format!("Project_{}", i)).collect::<Vec<_>>();

    let projects_per_user = 7;
    let projects_count = project_names.len();
    // Users are created here and not inside simulation based on hyperparameters
    // because there is an element of random in creation (e.g. Behavior), and
    // we would like all simulation to be conducted with the same set of users.
    let users = User::create_users(random, 10, projects_count, projects_per_user);


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
        request_amount: 8 * 512,
        // input_intensity: None,
        input_intensity: Some(0.85 * max_processing_intensity),
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

fn describe_simulation_hyperparameters(SimulationHyperParameters{ users, project_names, .. }: &SimulationHyperParameters) {
    println!(":> Users:");
    for user in users.iter() { describe_user(&user); }

    println!("Total count of project units: {}", project_names.len() * users[0].project_ids.len());
    println!();
}

fn describe_simulation_output(SimulationOutput{ start: _, duration, processed_user_requests, dbs_for_project, average_db_ping_millis }: &SimulationOutput) {
    /*
     * The (assigned_at - received_at) time generally can never be greater than
     * the waiting time for the fastest worker, so this metric is useless as a
     * waiting_time metric.
     */
    println!(":> Processed UserRequests statistics:");
    let mut average_total_time = 0;
    let mut average_waiting_time = 0;
    let mut worker_usage_count = Vec::new(); // empirically determines the amount of Workers
    // for UserRequest{ created_at, received_at, assigned_at, finished_at, processed_at_worker, ping_lasted, id, user_id, project_id, .. } in processed_user_requests {
    for UserRequest{ created_at, received_at, assigned_at, finished_at, processed_at_worker, ping_lasted: _, id: _, user_id: _, project_id: _, .. } in processed_user_requests {
        let received_at =         received_at        .expect("empty Option while describing processed request");
        let assigned_at =         assigned_at        .expect("empty Option while describing processed request");
        let finished_at =         finished_at        .expect("empty Option while describing processed request");
        // let ping_lasted =         ping_lasted        .expect("empty Option while describing processed request");
        let processed_at_worker = processed_at_worker.expect("empty Option while describing processed request");

        let waiting_time = assigned_at.duration_since(&*created_at).as_millis();
        let total_time   = finished_at.duration_since(&received_at).as_millis();
        average_total_time   += total_time;
        average_waiting_time += waiting_time;
        // waiting_times_bad.push(waiting_time);

        // println!("Request [{:>4}] with project {:>2} from {:>7} waited for {:>7} millis, processed in {:>8} millis, processed at {}, ping lasted {}ms",
        //     id,
        //     project_id,
        //     "#".repeat(*user_id as usize),
        //     waiting_time,
        //     total_time,
        //     processed_at_worker,
        //     ping_lasted.as_millis(),
        // );

        while processed_at_worker >= worker_usage_count.len() {
            worker_usage_count.push(0);
        }
        worker_usage_count[processed_at_worker] += 1;
    }

    let mut waiting_times_by_id = vec![0; processed_user_requests.len()];
    let mut waiting_times_by_iteration = vec![0; processed_user_requests.len()];
    let mut per_user_waiting_times_id = Vec::new();
    let mut per_user_waiting_times_iteration = Vec::new();
    let mut spread_moments_by_id = Vec::new();
    let mut spread_moments_by_iteration = Vec::new();
    let mut per_user_spread_id = Vec::new();
    let mut per_user_spread_iteration = Vec::new();
    for UserRequest{ user_id, created_at, assigned_at, id, processed_at_iteration, triggered_spread, .. } in processed_user_requests {
        let assigned_at = assigned_at.expect("empty Option while describing processed request");
        let waiting_time = assigned_at.duration_since(&*created_at).as_millis();
        waiting_times_by_id[*id as usize] = waiting_time;
        waiting_times_by_iteration[*processed_at_iteration] = waiting_time;

        while *user_id as usize >= per_user_waiting_times_id.len() {
            per_user_waiting_times_id.push(vec![0; processed_user_requests.len()]);
            per_user_spread_id.push(Vec::new());
        }
        per_user_waiting_times_id[*user_id as usize][*id as usize] = waiting_time;

        while *user_id as usize >= per_user_waiting_times_iteration.len() {
            per_user_waiting_times_iteration.push(vec![0; processed_user_requests.len()]);
            per_user_spread_iteration.push(Vec::new());
        }
        per_user_waiting_times_iteration[*user_id as usize][*processed_at_iteration] = waiting_time;

        if *triggered_spread {
            spread_moments_by_id.push(*id as u128);
            spread_moments_by_iteration.push(*processed_at_iteration as u128);
            per_user_spread_iteration[*user_id as usize].push(*processed_at_iteration as u128);
            per_user_spread_id[*user_id as usize].push(*id as u128);
        }
    }

    println!(":> Simulation finished in {} seconds", duration.as_secs());
    let processing_intensity = 1000.0 * processed_user_requests.len() as f32 / duration.as_millis() as f32;
    println!(":> Simulation processing intensity is {} requests per second ({} millis per request)",
        processing_intensity, (1000.0 / processing_intensity) as u32);
    average_total_time   /= processed_user_requests.len() as u128;
    average_waiting_time /= processed_user_requests.len() as u128;
    println!(":> Average total processing time = {} millis", average_total_time);
    println!(":> Average waiting time = {} millis", average_waiting_time);
    println!(":> Usage count of workers: {:?}", worker_usage_count);
    let db_len = average_db_ping_millis.len();
    println!(":> Database worktime: {:?}",
        average_db_ping_millis.into_iter().zip(worker_usage_count.into_iter()).map(|(ping, count)| ping * count as Time).collect::<Vec<_>>());
    let projects_for_db = (0..db_len).into_iter()
        .map(|i| {
            let mut res = Vec::new();
            for (project, dbs) in dbs_for_project.iter() {
                if dbs.contains(&i) {
                    res.push(project);
                }
            }
            res
        }).collect::<Vec<_>>();
    println!(":> Total project units stored: {}", projects_for_db.iter().map(|v| v.iter().count()).sum::<usize>());
    println!(":> Database usage by projects: {:?}", projects_for_db);

    // Generate charts
    draw_chart(waiting_times_by_id, Some(&spread_moments_by_id), "Waiting times by id", "request id", "waiting time").expect("Unable to build chart");
    // draw_chart(waiting_times_by_iteration, Some(&spread_moments_by_iteration), "Waiting times by iteration", "iteration", "waiting time").expect("Unable to build chart");
    // for (i, times_for_user) in per_user_waiting_times_id.into_iter().enumerate() {
    //     draw_chart(times_for_user, Some(&per_user_spread_id[i]), &format!("Waiting times by id for user {}", i), "request id", "waiting time").expect("Unable to build chart");
    // }
    // for (i, times_for_user) in per_user_waiting_times_iteration.into_iter().enumerate() {
    //     draw_chart(times_for_user, Some(&per_user_spread_iteration[i]), &format!("Waiting times by iteration for user {}", i), "iteration", "waiting time").expect("Unable to build chart");
    // }

    // for UserRequest{ created_at, received_at, id, .. } in processed_user_requests {
    //     println!("[{}] Created = {}, Received = {}", id, created_at.duration_since(&*start).as_micros(), received_at.unwrap().duration_since(&*start).as_micros());
    // }

    println!();
}

fn draw_chart(arr: Vec<u128>, marked: Option<&Vec<u128>>, name: &str, x_axis_name: &str, y_axis_name: &str) -> Option<()> {
    use plotters::prelude::*;
    const WIDTH: u32 = 1900;
    const HEIGHT: u32 = 300;
    let max_x = arr.len() as u128;
    let max_y = arr.iter().max().unwrap() + 100;

    let dots = arr.into_iter().enumerate()
        .map(|(i, elem)| (i as u128, elem))
        .collect::<Vec<_>>();

    let mut camel = name.to_string().replace(" ", "_");
    camel.make_ascii_lowercase();
    let img_path = format!("{}.png", camel);
    let root = BitMapBackend::new(&img_path, (WIDTH, HEIGHT)).into_drawing_area();
    root.fill(&WHITE).ok()?;

    let mut chart = ChartBuilder::on(&root)
        .caption(name, ("sans-serif", 20).into_font())
        .margin(10)
        .x_label_area_size(30)
        .y_label_area_size(50)
        .build_cartesian_2d(0..max_x, 0..max_y).ok()?;

    chart.configure_mesh()
        .x_desc(x_axis_name)
        .y_desc(y_axis_name)
        .draw().ok()?;

    chart.draw_series(LineSeries::new(
            dots.iter().map(|(x, y)| (*x, *y)), &BLUE)).ok()?;

    if let Some(moments) = marked {
        for moment in moments.iter() {
            chart.draw_series(LineSeries::new(vec![(*moment, 0), (*moment, max_y)], &RED)).ok()?;
        }
    }

    Some(())
}
// ============================================================================
// ============================================================================
// ============================================================================
#[tokio::main]
async fn main() -> MongoResult<()> {
    let simulation_is_real = false;

    let mut random = ChaChaRng::seed_from_u64(317);

    let hyperparameters = get_hyperparameters(&mut random, simulation_is_real).await?;
    let parameters = get_parameters();

    describe_simulation_hyperparameters(&hyperparameters);

    let output = if simulation_is_real {
        simulate(random, parameters, hyperparameters).await?
    } else {
        simulate_fake(random, parameters, hyperparameters)
    };

    describe_simulation_output(&output);

    Ok(())
}
