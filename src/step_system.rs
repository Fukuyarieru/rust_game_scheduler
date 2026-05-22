use std::{
    sync::{
        atomic::Ordering::SeqCst,
        mpsc::{Receiver, Sender, channel},
    },
    time::{Duration, Instant},
};

use crate::{
    worker::{Job, WorkResult, Worker},
    worker_pool::WorkerPool,
};

pub struct StepSystem {
    settings: StepSystemSettings,
    last_step: Step,

    pub worker_pool: WorkerPool,

    // workers: Vec<Worker>,
    // // job delagation
    // work_receiver: Receiver<Job>,
    // work_giver: Sender<Job>,
    // channel
    result_receiver: Receiver<Result<WorkResult, ()>>,
    result_sender: Sender<Result<WorkResult, ()>>,
    // //
    // signal_sender: Sender<GameStepSignal>,
    // signal_receiver: Receiver<GameStepSignal>,
}

// pub enum GameStepSignal {
//     FullyUsed,
//     Idle,
// }

impl StepSystem {
    // TODO: implement step waiting/stalling so to satisfy a consistant amount of actions desired
    // TODO: add WorkResult scoring to also affect every worker's rating, and a rate fixer for during idle times to not have phantom ratings
    pub fn run(&mut self) {
        // let times = self.settings.steps_per_second;
        match self.settings.steps_per_second_limit {
            Some(steps) => {
                let delay_per_step = 1000.0 / steps as f32;
            }
            None => {}
        };

        // while let Ok(job) = self.work_receiver.recv() {
        //     self.delegate(job);
        //     self.last_step = self.last_step.next();
        //     #[cfg(debug_assertions)]
        //     println!("{:?}", self.last_step)
        // }

        // loop {
        //     for i in 0..times {
        //         println!("{}: {:?}", i, self.last_step);
        //         self.last_step = self.last_step.next();
        //         std::thread::sleep(Duration::from_millis(delay_per_step as u64));
        //     }
        // }
    }
    // pub fn start_idle_workers(&mut self) {
    //     for worker in self.workers.iter_mut() {
    //         if !worker.running.load(SeqCst) {
    //             worker.start_working();
    //         }
    //     }
    // }
    // fn delegate(&mut self, job: Job) {
    //     let worker = self
    //         .workers
    //         .iter_mut()
    //         .min_by_key(|worker| worker.workload_rating.load(SeqCst))
    //         .unwrap();
    //     #[cfg(debug_assertions)]
    //     println!(
    //         "given worker {} job num {}, queue size {}",
    //         worker.id(),
    //         job.id(),
    //         worker.workload_rating.load(SeqCst)
    //     );
    //     worker.add(job);
    //     self.signal_appropriatly();
    // }

    // fn mass_delegate(&mut self, jobs: &[Job]) {
    //     todo!()
    // }
    // pub fn signal_appropriatly(&self) {
    //     if self
    //         .workers
    //         .iter()
    //         .all(|worker| worker.running.load(SeqCst))
    //     {
    //         self.signal_sender.send(GameStepSignal::FullyUsed);
    //     } else if self
    //         .workers
    //         .iter()
    //         .all(|worker| !worker.running.load(SeqCst))
    //     {
    //         self.signal_sender.send(GameStepSignal::Idle);
    //     }
    // }

    pub fn new(settings: StepSystemSettings) -> Self {
        // let mut workers = Vec::new();
        // let (w_s, w_r) = channel();
        let (r_s, r_r) = channel();
        // let (s_s, s_r) = channel();
        // for i in 0..settings.workers_count {
        //     workers.push(Worker::new(i as u128, r_s.clone()));
        // }
        Self {
            worker_pool: WorkerPool::new(settings.workers_count.clone()),
            settings,
            last_step: Step::new(),
            // work_receiver: w_r,
            // work_giver: w_s,
            result_receiver: r_r,
            result_sender: r_s,
            // workers,
            // signal_receiver: s_r,
            // signal_sender: s_s,
        }
    }
}

#[derive(Debug)]
pub struct Step {
    pub step_num: usize,
    pub time: Instant,
    pub difference: Duration,
}

pub struct StepSystemSettings {
    pub steps_per_second_limit: Option<usize>,
    pub workers_count: usize,
}

impl Step {
    pub fn next(&mut self) -> Self {
        let now = Instant::now();
        let dt = now.duration_since(self.time);
        Self {
            step_num: self.step_num + 1,
            time: now,
            difference: dt,
        }
    }
    pub fn new() -> Self {
        Self {
            step_num: 0,
            time: Instant::now(),
            difference: Duration::ZERO,
        }
    }
}
