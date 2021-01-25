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
    // Amount = 2
}

impl UserBehavior {
    fn gen(&self, projects: &Vec<ProjectId>) -> (ProjectId, UserBehavior) {
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

    // fn averaged_sequential() -> UserBehavior {
    //     
    // }
    // fn averaged_random() -> UserBehavior {
    //     
    // }

    fn random() -> UserBehavior {
        let enum_amount = 2; // XXX: UserBehavior enum amount
        match rand::thread_rng().gen_range(0..enum_amount) {
            0 => UserBehavior::AveragedSequential{ amount: /* any */ 0, left: 0, project_id: /* any */ 0 },
            1 => UserBehavior::AveragedRandom,
            _ => panic!("Bad range for UserBehavior random"),
        }
        // UserBehavior::AveragedSequential{ amount: /* any */ 0, left: 0, project_id: /* any */ 0 }
    }
}

fn subset_of_size(_size: usize, projects: &Vec<ProjectId>) -> Vec<ProjectId> {
    projects.clone()
}

struct User {
    user_behavior: UserBehavior,
    id: UserId,
    projects: Vec<ProjectId>,
}

impl User {
    // fn new(user_behaviour) -> UserBehaviorStream {
    //     UserBehaviorStream{ user_behavior, id, projects }
    // }

    fn gen(&mut self) -> ProjectId {
        let project_id;
        (project_id, self.user_behavior) = self.user_behavior.gen(&self.projects);
        project_id
    }

    fn create_users(users_amount: usize, projects: Vec<ProjectId>) -> Vec<User> {
        let projects_per_user = 5; // @hyper
        let mut vec = Vec::with_capacity(users_amount);
        for id in 0..users_amount {
            let user_behavior = UserBehavior::random();
            let user_projects = subset_of_size(projects_per_user, &projects);
            vec.push(User{ user_behavior, id: id as UserId, projects: user_projects });
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

// impl UserRequest {
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
// }
// ============================================================================
// ============================================================================
// ============================================================================
#[tokio::main]
async fn main() -> Result<()> {
    // ======== Hyper-parameters ========
    let request_stream_lambda = 500.0;


    // let mut c = MyEnum::Second{a:1, b:2};
    // let content = match c {
    //     MyEnum::Second{ref mut a, ref mut b} => a,
    //     _ => unreachable!(),
    // };
    // *content += 1;


    let (spawner_tx, spawner_rx) = channel();
    // Responsible for spawning Tasks
    thread::spawn(move|| {
        let user_amount = 5; // @hyper
        let projects = vec![1,2,3,4,5,6,7,8,9,10]; // @hyper
        let mut users = User::create_users(user_amount, projects);

        let mut time = 0;
        loop {
            // Pick random user
            let len = users.len();
            let user = &mut users[rand::thread_rng().gen_range(0..len)];

            // Pick project for this User according to his Strategy
            let project_id = user.gen();

            // Send for execution
            spawner_tx.send(UserRequest{ id: user.id, project_id, time }).unwrap();

            // Go to sleep
            let sleep_duration = request_stream_lambda * 2.71828f32.powf(-2f32 * rand::thread_rng().sample::<f32, _>(Open01));
            thread::sleep(time::Duration::from_millis(sleep_duration as u64));
            // println!("Sleeping for {}ms...", sleep_duration);

            time += 1;
        }
    });


    loop {
        let request = spawner_rx.recv().expect("dead spawner_tx channel");
        println!("Got {:?}", request);
    }


    Ok(())
}
