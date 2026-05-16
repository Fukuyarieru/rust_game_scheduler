use std::{
    ops::Range,
    sync::mpsc::Sender,
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::{
    gamestep::{GameStepSettings, GameStepSystem},
    worker::Job,
};

mod gamestep;
mod worker;

fn main() {
    let mut system = GameStepSystem::new(GameStepSettings {
        workers_count: 30,
        steps_per_second_limit: Some(4),
    });
    system.start_idle_workers();
    let work_giver = system.work_giver();
    let job_senders = amount_of_jobs_senders(work_giver, 10, 100, 0..3);

    system.run();
}

fn amount_of_jobs_senders(
    job_sender: Sender<Job>,
    amount: usize,
    amount_of_jobs: usize,
    wait_range: Range<u64>,
) -> Vec<JoinHandle<()>> {
    let mut senders = Vec::with_capacity(amount);
    for idx in 0..amount {
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
    wait_range: Range<u64>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let mut counter = 0;
        for _ in 0..=amount_of_jobs {
            let wait_range_clone = wait_range.clone();
            let new_work = Job::new(
                Box::new(move || {
                    let delay = rand::random_range(wait_range_clone);
                    println!("[{}] | {}, waited {} seconds", thread_id, counter, delay);
                    thread::sleep(Duration::from_secs(delay));
                }),
                None,
            );
            _ = job_sender.send(new_work);
            counter += 1;
            thread::sleep(Duration::from_millis(25));
        }
    })
}
