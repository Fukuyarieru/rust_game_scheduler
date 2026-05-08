use std::{
    sync::{
        atomic::Ordering::SeqCst,
        mpsc::{Receiver, Sender, channel},
    },
    time::{Duration, Instant},
};

use crate::worker::{Work, Worker};

pub struct GameStepSystem {
    settings: GameStepSettings,
    last_step: GameStep,
    workers: Vec<Worker>,
    // work delagation
    work_receiver: Receiver<Work>,
    work_giver: Sender<Work>,
    // channel
    result_receiver: Receiver<(usize, Duration)>,
    result_sender: Sender<(usize, Duration)>,
}

impl GameStepSystem {
    pub fn run(&mut self) {
        let times = self.settings.steps_per_second;
        let delay_per_step = 1000.0 / times as f32;

        while let Ok(work) = self.work_receiver.recv() {
            self.delegate(work);
        }

        // loop {
        //     for i in 0..times {
        //         println!("{}: {:?}", i, self.last_step);
        //         self.last_step = self.last_step.next();
        //         std::thread::sleep(Duration::from_millis(delay_per_step as u64));
        //     }
        // }
    }
    pub fn start_idle_workers(&mut self) {
        for worker in self.workers.iter_mut() {
            if !worker.running.load(SeqCst) {
                worker.start_working();
            }
        }
    }
    fn delegate(&mut self, work: Work) {
        let worker = self
            .workers
            .iter_mut()
            .min_by_key(|worker| worker.workload_rating.load(SeqCst))
            .unwrap();
        #[cfg(debug_assertions)]
        println!("given worker {} work num {}", worker.id(), work.id());
        worker.add(work);
    }
    pub fn new(settings: GameStepSettings) -> Self {
        let mut workers = Vec::new();
        let (w_s, w_r) = channel();
        let (r_s, r_r) = channel();
        for i in 0..settings.workers_count {
            workers.push(Worker::new(i, r_s.clone()));
        }
        Self {
            settings,
            last_step: GameStep::new(),
            work_receiver: w_r,
            work_giver: w_s,
            result_receiver: r_r,
            result_sender: r_s,
            workers,
        }
    }
    pub fn work_giver(&self) -> Sender<Work> {
        self.work_giver.clone()
    }
}

#[derive(Debug)]
pub struct GameStep {
    pub step_num: usize,
    pub time: Instant,
    pub difference: Duration,
}

pub struct GameStepSettings {
    pub steps_per_second: usize,
    pub workers_count: usize,
}

impl GameStep {
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
