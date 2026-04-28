use crate::gamestep::{GameStepSettings, GameStepSystem};

mod gamestep;

fn main() {
    let mut system = GameStepSystem::new(GameStepSettings {
        workers_count: 4,
        steps_per_second: 4,
    });
    system.run();
}
