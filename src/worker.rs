use std::{
    ops::{AddAssign, SubAssign},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        mpsc::{Receiver, Sender, channel},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};
pub struct Worker {
    id: usize,
    pub running: Arc<AtomicBool>,
    // passed in and out of work thread
    work_left: Option<Receiver<Work>>,
    _work_sender: Sender<Work>,
    //
    // (id,run duration)
    work_result_sender: Sender<(usize, Duration)>,
    //
    thread: Option<JoinHandle<Receiver<Work>>>,
    //
    pub workload_rating: Arc<AtomicUsize>,
}

pub struct Work {
    id: usize,
    task: Box<dyn FnOnce() + 'static + Send>,
}
impl Work {
    pub fn run(self) -> (usize, Duration) {
        let i = Instant::now();
        (self.task)();
        (self.id, i.elapsed())
    }
}

impl Worker {
    pub fn new(id: usize, work_result_sender: Sender<(usize, Duration)>) -> Self {
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
        self.workload_rating.load(SeqCst).add_assign(1);
    }
    pub fn start_working(&mut self) {
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
                rating.load(SeqCst).sub_assign(1);
            }
            r
        }));
    }
    pub fn stop_working(&mut self) {
        self.running.store(false, SeqCst);
        let r = self.thread.take().unwrap().join().unwrap();
        self.work_left = Some(r);
    }
    pub fn id(&self) -> usize {
        self.id
    }
}
