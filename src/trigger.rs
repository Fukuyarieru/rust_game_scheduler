use std::{
    sync::mpsc::{Receiver, Sender, channel},
    time::{Duration, Instant},
};

/// Condition which will be checked and looped untill fired
pub struct Trigger {
    trigger_fire: Sender<()>,
    trigger_listener: Receiver<()>,
    /// Time limit for wait loop
    timeout: Option<Duration>,
}

struct Fire(Sender<()>);

impl Trigger {
    pub fn new(timeout: Option<Duration>) -> (Self, Fire) {
        let (s, r) = channel();
        (
            Self {
                trigger_fire: s.clone(),
                trigger_listener: r,
                timeout,
            },
            Fire(s),
        )
    }
    pub fn fire(&self) -> Fire {
        Fire(self.trigger_fire.clone())
    }
    pub fn wait(&self) -> Result<(), ()> {
        if let Some(timeout) = self.timeout {
            let instant = Instant::now();
            while timeout < Instant::now().duration_since(instant) {
                if let Ok(()) = self.trigger_listener.try_recv() {
                    return Ok(());
                };
            }
            return Err(());
        } else {
            while let Ok(_) = self.trigger_listener.recv() {
                break;
            }
        }
        Ok(())
    }
    // pub fn check(&self) -> Result<(), ()> {
    //     self.trigger_listener.try_recv()
    // }
}

impl Fire {
    pub fn fire(self) {
        _ = self.0.send(());
    }
}
