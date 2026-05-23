use std::{
    ops::{Range, RangeInclusive},
    sync::mpsc::Sender,
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::{
    step_system::{StepSystem, StepSystemSettings},
    worker::Job,
};

mod step_system;
mod trigger;
mod worker;
mod worker_pool;

fn main() {
    let mut system = StepSystem::new(StepSystemSettings {
        workers_count: 3,
        // TODO: 100 got delay, 200 doesnt, thats because of the division and type conversions
        steps_per_second_limit: Some(100),
    });

    let work_giver = system.job_giver();
    let job_senders = amount_of_jobs_senders(work_giver, 3, 100, 0..=0);

    system.run();
}

fn amount_of_jobs_senders(
    job_sender: Sender<Job>,
    amount_of_senders: usize,
    amount_of_jobs: usize,
    wait_range: RangeInclusive<u64>,
) -> Vec<JoinHandle<()>> {
    let mut senders = Vec::with_capacity(amount_of_senders);
    for idx in 0..amount_of_senders {
        senders.push(give_jobs_thread(
            job_sender.clone(),
            idx,
            amount_of_jobs,
            wait_range.clone(),
        ));
    }
    senders
}

fn give_jobs_thread(
    job_sender: Sender<Job>,
    thread_id: usize,
    amount_of_jobs: usize,
    wait_range: RangeInclusive<u64>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut counter = 0;
        for _ in 0..=amount_of_jobs {
            let wait_range_clone = wait_range.clone();
            let new_work = Job::new(
                Box::new(move || {
                    let delay = rand::random_range(wait_range_clone);
                    println!(
                        "[thread id: {}] | counter: {}, waited {} millis",
                        thread_id, counter, delay
                    );
                    thread::sleep(Duration::from_millis(delay));
                }),
                None,
            );
            _ = job_sender.send(new_work);
            counter += 1;
            thread::sleep(Duration::from_millis(25));
        }
    })
}
