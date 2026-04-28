use std::{
    ops::{AddAssign, Deref, DerefMut, SubAssign},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst},
        mpsc::{Receiver, Sender, channel},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

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

        while let Ok(work) = self.work_receiver.recv() {}

        // loop {
        //     for i in 0..times {
        //         println!("{}: {:?}", i, self.last_step);
        //         self.last_step = self.last_step.next();
        //         std::thread::sleep(Duration::from_millis(delay_per_step as u64));
        //     }
        // }
    }

    fn delegate(&mut self, work: Work) {
        if let Some(worker) = self
            .workers
            .iter_mut()
            .find(|worker| !worker.running.load(SeqCst))
        {
            worker.start_working();
            worker.add(work);
        } else {
            self.workers
                .iter_mut()
                .min_by_key(|worker| worker.workload_rating.load(SeqCst))
                .unwrap()
                .add(work);
        }
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
