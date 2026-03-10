use std::sync::atomic::{AtomicI32, Ordering};

use crate::error::{Error, Result};

/// Lifecycle state of the WAL.
///
/// Progresses strictly forward: `Init → Running → Draining → Closed`.
/// Backward transitions are never allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum State {
    /// Allocated but writer thread not yet started.
    Init = 0,
    /// Fully operational: accepting appends and serving replays.
    Running = 1,
    /// Graceful shutdown in progress. New appends are rejected; the writer
    /// thread drains remaining batches.
    Draining = 2,
    /// Terminal state. All resources released.
    Closed = 3,
}

impl State {
    fn from_i32(v: i32) -> Self {
        match v {
            0 => Self::Init,
            1 => Self::Running,
            2 => Self::Draining,
            3 => Self::Closed,
            _ => Self::Closed,
        }
    }
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Init => f.write_str("INIT"),
            Self::Running => f.write_str("RUNNING"),
            Self::Draining => f.write_str("DRAINING"),
            Self::Closed => f.write_str("CLOSED"),
        }
    }
}

/// Atomic state machine enforcing valid forward-only lifecycle transitions.
pub(crate) struct StateMachine(AtomicI32);

impl StateMachine {
    pub const fn new() -> Self {
        Self(AtomicI32::new(State::Init as i32))
    }

    /// Returns the current state.
    #[inline]
    pub fn load(&self) -> State {
        State::from_i32(self.0.load(Ordering::Acquire))
    }

    /// Attempts an atomic CAS transition from `from` to `to`.
    /// Returns `true` on success.
    pub fn transition(&self, from: State, to: State) -> bool {
        if !valid_transition(from, to) {
            return false;
        }
        self.0
            .compare_exchange(from as i32, to as i32, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Returns `Ok(())` if the WAL is in `Running`, or the appropriate error.
    pub fn must_be_running(&self) -> Result<()> {
        match self.load() {
            State::Running => Ok(()),
            State::Draining => Err(Error::Draining),
            State::Closed => Err(Error::Closed),
            _ => Err(Error::NotRunning),
        }
    }
}

fn valid_transition(from: State, to: State) -> bool {
    matches!(
        (from, to),
        (State::Init, State::Running)
            | (State::Running, State::Draining)
            | (State::Draining, State::Closed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state() {
        let sm = StateMachine::new();
        assert_eq!(sm.load(), State::Init);
    }

    #[test]
    fn valid_forward_transitions() {
        let sm = StateMachine::new();
        assert!(sm.transition(State::Init, State::Running));
        assert_eq!(sm.load(), State::Running);

        assert!(sm.transition(State::Running, State::Draining));
        assert_eq!(sm.load(), State::Draining);

        assert!(sm.transition(State::Draining, State::Closed));
        assert_eq!(sm.load(), State::Closed);
    }

    #[test]
    fn backward_transition_rejected() {
        let sm = StateMachine::new();
        sm.transition(State::Init, State::Running);
        assert!(!sm.transition(State::Running, State::Init));
        assert_eq!(sm.load(), State::Running);
    }

    #[test]
    fn skip_transition_rejected() {
        let sm = StateMachine::new();
        assert!(!sm.transition(State::Init, State::Closed));
        assert_eq!(sm.load(), State::Init);
    }

    #[test]
    fn wrong_from_state_rejected() {
        let sm = StateMachine::new();
        assert!(!sm.transition(State::Running, State::Draining));
        assert_eq!(sm.load(), State::Init);
    }

    #[test]
    fn must_be_running() {
        let sm = StateMachine::new();
        assert_eq!(sm.must_be_running().unwrap_err(), Error::NotRunning);

        sm.transition(State::Init, State::Running);
        assert!(sm.must_be_running().is_ok());

        sm.transition(State::Running, State::Draining);
        assert_eq!(sm.must_be_running().unwrap_err(), Error::Draining);

        sm.transition(State::Draining, State::Closed);
        assert_eq!(sm.must_be_running().unwrap_err(), Error::Closed);
    }

    #[test]
    fn display() {
        assert_eq!(State::Init.to_string(), "INIT");
        assert_eq!(State::Running.to_string(), "RUNNING");
        assert_eq!(State::Draining.to_string(), "DRAINING");
        assert_eq!(State::Closed.to_string(), "CLOSED");
    }
}
