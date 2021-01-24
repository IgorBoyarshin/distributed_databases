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


use std::thread;
use std::time;
use std::sync::mpsc::channel;
// ============================================================================
// ============================================================================
// ============================================================================



enum UserBehavior {
    AveragedSequential {
        amount: u32,
        left: u32,
        project_id: ProjectId,
    },
    AveragedRandom,
    // PreferentialMono {
    //     project_id: ProjectId,
    // },
    // PreferentialDuo,
}

impl UserBehavior {
    fn gen(self, projects: &Vec<ProjectId>) -> (ProjectId, UserBehavior) {
        match self {
            UserBehavior::AveragedSequential{ mut amount, mut left, mut project_id } => {
                let max_amount = 10; // @hyper

                if left == 0 {
                    amount     = rand::thread_rng().gen_range(1..=max_amount);
                    project_id = projects[rand::thread_rng().gen_range(0..projects.len())];
                    left       = amount;
                }
                left -= 1;

                (project_id, UserBehavior::AveragedSequential{ amount, left, project_id })
            },
            UserBehavior::AveragedRandom => {
                let project_id = projects[rand::thread_rng().gen_range(0..projects.len())];
                (project_id, UserBehavior::AveragedRandom)
            },
        }
    }

    fn new_averaged_sequential() -> UserBehavior {
        UserBehavior::AveragedSequential{ amount: /* any */ 0, left: 0, project_id: /* any */ 0 }
    }
}

struct UserRequestStream {
    user_behaviour: UserBehavior,
    id: UserId,
    projects: Vec<ProjectId>,
}

// impl Iterator for UserRequestStream {
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



type ProjectId = u32;
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

impl UserRequest {
    // fn for_behavior(behavior: UserBehavior) -> UserRequest {
    //
    // }
    // fn new(id: UserId, from: Location, time: Time) -> UserRequest {
    //     let max_action_time = 10;
    //     let action_time = rand::thread_rng().gen_range(1..=max_action_time);
    //     let operation = Operation::new(OperationType::random(), action_time);
    //     UserRequest{ id, from, operation, time }
    // }
    // fn new(id: UserId, time: Time) -> UserRequest {
    //     UserRequest{ id, time }
    // }
}
// ============================================================================
// ============================================================================
// ============================================================================
#[tokio::main]
async fn main() -> Result<()> {
    // ======== Hyper-parameters ========
    let request_stream_lambda = 500.0;


    let (spawner_tx, spawner_rx) = channel();
    // Responsible for spawning Tasks
    thread::spawn(move|| {
        let projects = vec![1,2,3,4,5];
        let mut user = UserBehavior::new_averaged_sequential();
        // let mut user = UserBehavior::AveragedRandom;

        let mut time = 0;
        loop {
            // Pick random user
            let id = rand::thread_rng().gen_range(1..=10);
            let project_id;
            (project_id, user) = user.gen(&projects);

            let sleep_duration = request_stream_lambda * 2.71828f32.powf(-2f32 * rand::thread_rng().sample::<f32, _>(Open01));
            // println!("Sleeping for {}ms...", sleep_duration);
            thread::sleep(time::Duration::from_millis(sleep_duration as u64));

            spawner_tx.send(UserRequest{ id, project_id, time }).unwrap();

            time += 1;
        }
    });


    loop {
        let request = spawner_rx.recv().expect("dead spawner_tx channel");
        println!("Got {:?}", request);
    }


    Ok(())
}
