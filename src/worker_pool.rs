use std::sync::atomic::Ordering::SeqCst;
use std::{
    sync::{
        Arc,
        atomic::AtomicBool,
        mpsc::{Receiver, Sender, channel},
    },
    thread::{JoinHandle, spawn},
};

use crate::worker::{Job, WorkResult, Worker};

pub struct WorkerPool {
    workers: Option<Vec<Worker>>,
    // job delagation
    work_receiver: Option<Receiver<Job>>,
    work_giver: Sender<Job>,
    // channel
    pub result_receiver: Receiver<Result<WorkResult, ()>>,
    result_sender: Sender<Result<WorkResult, ()>>,
    // active
    active: Arc<AtomicBool>,
    delegator: Option<JoinHandle<(Receiver<Job>, Vec<Worker>)>>,
}

impl WorkerPool {
    pub fn new(worker_count: usize) -> Self {
        let mut workers = Vec::new();
        let (w_s, w_r) = channel();
        let (r_s, r_r) = channel();
        for i in 0..worker_count {
            workers.push(Worker::new(i as u128, r_s.clone()));
        }

        Self {
            workers: Some(workers),
            work_receiver: Some(w_r),
            work_giver: w_s,
            result_receiver: r_r,
            result_sender: r_s,
            active: Arc::new(AtomicBool::new(false)),
            delegator: None,
        }
    }
    pub fn on(&mut self) {
        let mut workers = self.workers.take().unwrap();
        self.active.store(true, SeqCst);
        let active = self.active.clone();
        let receiver = self.work_receiver.take().unwrap();
        self.delegator = Some(spawn(move || {
            while active.load(SeqCst)
                && let Ok(job) = receiver.recv()
            {
                let worker = workers
                    .iter_mut()
                    .min_by_key(|worker| worker.workload_rating.load(SeqCst))
                    .unwrap();
                #[cfg(debug_assertions)]
                println!(
                    "given worker {} job num {}, queue size {}",
                    worker.id(),
                    job.id(),
                    worker.workload_rating.load(SeqCst)
                );
                _ = worker.add(job);
            }
            (receiver, workers)
        }));
    }
    pub fn off(&mut self) {
        self.active.store(false, SeqCst);
        let delegator = self.delegator.take();
        let handle = delegator.unwrap();
        let (receiver, workers) = handle.join().unwrap();
        self.work_receiver = Some(receiver);
        self.workers = Some(workers);
    }
    pub fn work_giver(&self) -> Sender<Job> {
        self.work_giver.clone()
    }
    /// MUST BE DONE WHILE ON "OFF" MODE
    pub fn worker_running_status(&self, idx: usize) -> bool {
        self.workers.as_ref().unwrap()[idx].running.load(SeqCst)
    }
    /// MUST BE DONE WHILE ON "OFF" MODE
    pub fn change_worker_running_status(&self, idx: usize, state: bool) {
        self.workers.as_ref().unwrap()[idx]
            .running
            .store(state, SeqCst);
    }
    /// MUST BE DONE WHILE ON "OFF" MODE
    pub fn change_all_workers_running_status(&self, state: bool) {
        self.workers
            .as_ref()
            .unwrap()
            .iter()
            .for_each(|worker| worker.running.store(state, SeqCst));
    }
}
