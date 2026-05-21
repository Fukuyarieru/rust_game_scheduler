use std::{
    sync::mpsc::{Receiver, Sender},
    time::{Duration, Instant},
};

/// Condition which will be checked and looped untill fired
pub struct Trigger {
    trigger_fire: Sender<()>,
    pub trigger_listener: Receiver<()>,
    /// Time limit for wait loop
    timeout: Option<Duration>,
}

struct Fire(Sender<()>);

impl Trigger {
    pub fn fire(&self) -> Fire {
        Fire(self.trigger_fire.clone())
    }
    pub fn check(&self) -> Result<(), ()> {
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
}

impl Fire {
    pub fn fire(self) {
        _ = self.0.send(());
    }
}
