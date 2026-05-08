use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        mpsc::{Receiver, Sender, channel},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use uuid::Uuid;
pub struct Worker {
    id: u128,
    pub running: Arc<AtomicBool>,
    // passed in and out of work thread
    work_left: Option<Receiver<Work>>,
    _work_sender: Sender<Work>,
    //
    // (id,run duration)
    work_result_sender: Sender<Result<WorkResult, ()>>,
    //
    thread: Option<JoinHandle<Receiver<Work>>>,
    //
    pub workload_rating: Arc<AtomicUsize>,
}
pub struct Work {
    id: u128,
    task: Box<dyn FnOnce() + 'static + Send>,
    trigger: Option<Trigger>,
}
pub struct Trigger {
    sender: Sender<()>,
    pub receiver: Receiver<()>,
    timeout: Option<Duration>,
}
pub struct WorkResult {
    id: u128,
    duration: Duration,
}

impl Trigger {
    pub fn sender(&self) -> Sender<()> {
        self.sender.clone()
    }
    pub fn wait(&self) -> Result<(), ()> {
        if let Some(timeout) = self.timeout {
            let instant = Instant::now();
            while timeout < Instant::now().duration_since(instant) {
                if let Ok(()) = self.receiver.try_recv() {
                    return Ok(());
                };
            }
            return Err(());
        } else {
            while let Ok(_) = self.receiver.recv() {
                break;
            }
        }
        Ok(())
    }
}
impl Work {
    pub fn run(self) -> Result<WorkResult, ()> {
        let i = Instant::now();
        if let Some(trigger) = self.trigger {
            match trigger.wait() {
                Ok(_) => {}
                Err(_) => return Err(()),
            };
        }
        (self.task)();
        Ok(WorkResult {
            id: self.id,
            duration: i.elapsed(),
        })
    }
    pub fn new(task: Box<dyn FnOnce() + 'static + Send>, trigger: Option<Trigger>) -> Self {
        Self {
            id: Uuid::new_v4().as_u128(),
            task: task,
            trigger,
        }
    }
    pub fn id(&self) -> u128 {
        self.id
    }
}

impl Worker {
    pub fn new(id: u128, work_result_sender: Sender<Result<WorkResult, ()>>) -> Self {
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
    pub fn add(&mut self, work: Work) {
        self._work_sender.send(work).unwrap();
        self.workload_rating.fetch_add(1, SeqCst);
    }
    pub fn start_working(&mut self) {
        self.running.store(true, SeqCst);

        let running = self.running.clone();
        let r = self.work_left.take().unwrap();
        let result_sender = self.work_result_sender.clone();
        let rating = self.workload_rating.clone();
        self.thread = Some(std::thread::spawn(move || {
            while let Ok(work) = r.recv()
                && running.load(SeqCst)
            {
                let result = work.run();
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
    pub fn id(&self) -> u128 {
        self.id
    }
}
