//! The in-flight ceiling: at most 24 requests at once in one isolate, the
//! reference's `standard` profile (`StorageServer.ts:320-327, 475`;
//! `edgePolicy.ts:656-688`), answered `503 ERR_SERVER_BUSY` with
//! `Retry-After: 1` beyond it.
//!
//! A Worker's request future can be abandoned without being dropped (a client
//! that goes away), so a slot also expires after the reference's request
//! timeout of two minutes (`StorageServer.ts:329`): a lost slot costs the
//! isolate one place for at most that long, never for good.

use std::cell::RefCell;

use super::Refusal;

/// The ceiling on requests in flight in one isolate.
pub const MAX_IN_FLIGHT: usize = 24;

/// How long a slot is held at most, in milliseconds (the reference's request timeout).
pub const SLOT_TTL_MS: f64 = 120_000.0;

/// The slots in flight: (slot id, entered at, in milliseconds since the epoch).
#[derive(Debug, Default)]
pub struct Slots {
    next_id: u64,
    held: Vec<(u64, f64)>,
}

impl Slots {
    /// Take a slot at `now_ms`, after releasing every slot older than the TTL;
    /// `None` when the ceiling is reached.
    pub fn enter(&mut self, now_ms: f64) -> Option<u64> {
        self.held.retain(|(_, at)| now_ms - at < SLOT_TTL_MS);
        if self.held.len() >= MAX_IN_FLIGHT {
            return None;
        }
        self.next_id = self.next_id.wrapping_add(1);
        self.held.push((self.next_id, now_ms));
        Some(self.next_id)
    }

    /// Give a slot back.
    pub fn leave(&mut self, id: u64) {
        self.held.retain(|(held, _)| *held != id);
    }

    /// The slots held now.
    pub fn len(&self) -> usize {
        self.held.len()
    }

    /// Whether no slot is held.
    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }
}

thread_local! {
    static SLOTS: RefCell<Slots> = RefCell::new(Slots::default());
}

/// A slot held for one request; dropping it gives the slot back.
#[derive(Debug)]
pub struct Slot(u64);

impl Drop for Slot {
    fn drop(&mut self) {
        SLOTS.with(|s| s.borrow_mut().leave(self.0));
    }
}

/// Take a slot for the request now, or the reference's 503.
pub fn enter(now_ms: f64) -> Result<Slot, Refusal> {
    SLOTS
        .with(|s| s.borrow_mut().enter(now_ms))
        .map(Slot)
        .ok_or_else(busy)
}

/// The reference's answer at the ceiling (`edgePolicy.ts:667-673`); the
/// caller adds `Retry-After: 1`.
pub fn busy() -> Refusal {
    Refusal::plain(
        503,
        "ERR_SERVER_BUSY",
        "The service is temporarily at capacity.",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_twenty_fifth_is_refused_and_a_release_admits_the_next() {
        let mut slots = Slots::default();
        let ids: Vec<u64> = (0..MAX_IN_FLIGHT)
            .map(|_| slots.enter(0.0).expect("under the ceiling"))
            .collect();
        assert_eq!(slots.enter(1.0), None);
        slots.leave(ids[3]);
        assert!(slots.enter(2.0).is_some());
        assert_eq!(slots.len(), MAX_IN_FLIGHT);
    }

    #[test]
    fn an_abandoned_slot_expires_at_the_request_timeout() {
        let mut slots = Slots::default();
        for _ in 0..MAX_IN_FLIGHT {
            slots.enter(0.0).expect("under the ceiling");
        }
        assert_eq!(slots.enter(SLOT_TTL_MS - 1.0), None);
        assert!(slots.enter(SLOT_TTL_MS).is_some());
        assert_eq!(slots.len(), 1);
    }

    #[test]
    fn the_guard_gives_its_slot_back() {
        let before = SLOTS.with(|s| s.borrow().len());
        {
            let _slot = enter(0.0).expect("a slot");
            assert_eq!(SLOTS.with(|s| s.borrow().len()), before + 1);
        }
        assert_eq!(SLOTS.with(|s| s.borrow().len()), before);
    }

    #[test]
    fn the_refusal_is_the_references() {
        let r = busy();
        assert_eq!(r.status, 503);
        assert_eq!(r.body["code"], "ERR_SERVER_BUSY");
        assert_eq!(
            r.body["description"],
            "The service is temporarily at capacity."
        );
    }
}
