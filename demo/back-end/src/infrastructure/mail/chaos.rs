//! DEMO ONLY — decides whether the mail server "fails" on this attempt.
use rand::{RngExt, SeedableRng, rngs::StdRng};

pub trait ChaosPolicy: Send {
    /// `true` ⇒ this attempt fails and the task is retried.
    fn should_fail(&mut self) -> bool;
    /// Picks one of `n` funny reasons.
    fn pick_reason(&mut self, n: usize) -> usize;
}

/// Never fails (tests and `ENCORE_TASK_FAILURE_RATE=0`).
pub struct Never;

impl ChaosPolicy for Never {
    fn should_fail(&mut self) -> bool {
        false
    }
    fn pick_reason(&mut self, _: usize) -> usize {
        0
    }
}

/// Always fails (tests).
pub struct Always;

impl ChaosPolicy for Always {
    fn should_fail(&mut self) -> bool {
        true
    }
    fn pick_reason(&mut self, _: usize) -> usize {
        0
    }
}

/// Fails with probability `p`; an explicit seed makes it deterministic.
pub struct Random {
    probability: f64,
    rng: StdRng,
}

impl Random {
    pub fn new(probability: f64, seed: Option<u64>) -> Self {
        let seed = seed.unwrap_or_else(|| rand::rng().random());
        Self {
            probability: probability.clamp(0.0, 1.0),
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl ChaosPolicy for Random {
    fn should_fail(&mut self) -> bool {
        self.probability > 0.0 && self.rng.random_bool(self.probability)
    }
    fn pick_reason(&mut self, n: usize) -> usize {
        if n == 0 { 0 } else { self.rng.random_range(0..n) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_random_policy_fails_about_twenty_percent_of_the_time() {
        let mut policy = Random::new(0.2, Some(42));
        let failures = (0..10_000).filter(|_| policy.should_fail()).count() as f64 / 10_000.0;
        assert!((0.18..=0.22).contains(&failures), "failure share {failures}");
        let mut replay = Random::new(0.2, Some(42));
        let again = (0..10_000).filter(|_| replay.should_fail()).count() as f64 / 10_000.0;
        assert_eq!(failures, again, "same seed, same dice");
        assert!(!Random::new(0.0, Some(1)).should_fail());
        assert!(Random::new(1.0, Some(1)).should_fail());
        assert!(Random::new(0.5, Some(7)).pick_reason(4) < 4);
        assert!(!Never.should_fail());
        assert!(Always.should_fail());
    }
}
