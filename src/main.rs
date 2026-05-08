use std::{thread, time::Duration};

use crate::{
    gamestep::{GameStepSettings, GameStepSystem},
    worker::Work,
};

mod gamestep;
mod worker;

fn main() {
    let mut system = GameStepSystem::new(GameStepSettings {
        workers_count: 40,
        steps_per_second: 4,
    });
    system.start_idle_workers();
    let work_giver = system.work_giver();
    thread::spawn(move || {
        let mut counter = 0;
        for _ in 0..500 {
            let new_work = Work::new(Box::new(move || {
                println!("{}", counter);
                thread::sleep(Duration::from_secs(rand::random_range(1..=7)));
            }));
            _ = work_giver.send(new_work);
            counter += 1;
            thread::sleep(Duration::from_millis(25));
            // thread::sleep(Duration::from_secs(1));
        }
    });
    system.run();
}
