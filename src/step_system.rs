use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::SeqCst},
        mpsc::{Receiver, Sender, channel},
    },
    thread,
    time::{Duration, Instant},
};

use crate::{
    worker::{Job, JobResult, Worker},
    worker_pool::WorkerPool,
};

pub struct StepSystem {
    pub settings: StepSystemSettings,
    last_step: Step,

    worker_pool: WorkerPool,

    job_giver: Sender<Job>,
    job_receiver: Receiver<Job>,
    result_receiver: Receiver<JobResult>,
    active: Arc<AtomicBool>,
    // workers: Vec<Worker>,
    // // job delagation
    // work_receiver: Receiver<Job>,
    // work_giver: Sender<Job>,
    // channel
    // result_receiver: Receiver<Result<WorkResult, ()>>,
    // result_sender: Sender<Result<WorkResult, ()>>,
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
        self.worker_pool.change_all_workers_running_status(true);
        self.worker_pool.on();

        self.active.store(true, SeqCst);

        let delay = if let Some(amount) = self.settings.steps_per_second_limit {
            // TODO: PROBLEM HERE
            Some(Duration::from_millis((100.0 / amount as f64) as u64 * 1000))
        } else {
            None
        };

        while self.active.load(SeqCst)
            && let Ok(job) = self.job_receiver.recv()
        {
            self.delegate(job);
            self.last_step = self.last_step.next();
            println!("{:?}", self.last_step);
            if let Some(delay) = delay {
                println!("WAITING {:?}", delay);
                // TODO: NEED TO FIX DELAY
                thread::sleep(delay);
            }
        }

        // loop {
        //     for i in 0..times {
        //         println!("{}: {:?}", i, self.last_step);
        //         self.last_step = self.last_step.next();
        //         std::thread::sleep(Duration::from_millis(delay_per_step as u64));
        //     }
        // }
    }

    pub fn job_giver(&self) -> Sender<Job> {
        self.job_giver.clone()
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

    pub fn delegate(&mut self, job: Job) {
        // CLONES CONSTANTLY
        _ = self.worker_pool.job_giver.send(job);
    }

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

        let (j_s, j_r) = channel();

        let worker_pool = WorkerPool::new(settings.workers_count);

        Self {
            worker_pool,
            settings,
            job_giver: j_s,
            job_receiver: j_r,
            last_step: Step::new(),
            result_receiver: r_r,
            // result_sender: r_s,
            active: Arc::new(AtomicBool::new(false)),
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
