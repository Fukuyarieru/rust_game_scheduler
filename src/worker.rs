use crate::trigger::Trigger;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        mpsc::{Receiver, SendError, Sender, channel},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
use uuid::Uuid;

pub struct Worker {
    id: u128,
    running: Arc<AtomicBool>,
    // passed in and out of job thread
    work_left: Option<Receiver<Job>>,
    _work_sender: Sender<Job>,
    //
    work_result_sender: Sender<Result<JobResult, ()>>,
    //
    thread: Option<JoinHandle<Receiver<Job>>>,
    //
    pub workload_rating: Arc<AtomicUsize>,
}
/// Code boxed to be ran
pub struct Job {
    /// Uuid
    id: u128,
    /// Code
    task: Box<dyn FnOnce() + 'static + Send>,
    trigger: Option<Trigger>,
}

pub struct JobResult {
    worker_id: u128,
    /// Uuid
    work_id: u128,
    /// Time took for the job to complete
    total_duration: Duration,
    /// Time waited for a trigger
    wait_duration: Duration,
}

impl Job {
    pub fn run(self, worker_id: u128) -> Result<JobResult, ()> {
        let i = Instant::now();
        if let Some(trigger) = self.trigger {
            match trigger.wait() {
                Ok(_) => {}
                Err(_) => return Err(()),
            };
        }
        let wait_duration = i.elapsed();
        (self.task)();
        Ok(JobResult {
            worker_id,
            work_id: self.id,
            total_duration: i.elapsed(),
            wait_duration,
        })
    }
    pub fn new(task: Box<dyn FnOnce() + 'static + Send>, trigger: Option<Trigger>) -> Self {
        Self {
            id: Uuid::new_v4().as_u128(),
            task: task,
            trigger,
        }
    }
    pub fn work_id(&self) -> u128 {
        self.id
    }
}

impl Worker {
    pub fn new(id: u128, work_result_sender: Sender<Result<JobResult, ()>>) -> Self {
        let (s, r) = channel();
        Self {
            id,
            running: Arc::new(AtomicBool::new(false)),
            work_left: Some(r),
            _work_sender: s,
            work_result_sender: work_result_sender,
            thread: None,
            workload_rating: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn add(&mut self, job: Job) -> Result<(), SendError<Job>> {
        self.workload_rating.fetch_add(1, SeqCst);
        self._work_sender.send(job)
    }
    pub fn start_working(&mut self) {
        self.running.store(true, SeqCst);
        let worker_id = self.id;

        let running = self.running.clone();
        let r = self.work_left.take().unwrap();
        let result_sender = self.work_result_sender.clone();
        let rating = self.workload_rating.clone();
        self.thread = Some(std::thread::spawn(move || {
            while running.load(SeqCst)
                && let Ok(job) = r.recv()
            {
                let result = job.run(worker_id);
                result_sender.send(result).unwrap();
                rating.fetch_sub(1, SeqCst);
            }
            r
        }));
    }
    pub fn stop_working(&mut self) {
        self.running.store(false, SeqCst);
        let r = self.thread.take().unwrap().join().unwrap();
        self.work_left = Some(r);
    }
    pub fn get_running_state(&self) -> bool {
        self.running.load(SeqCst)
    }
    pub fn id(&self) -> u128 {
        self.id
    }
}
