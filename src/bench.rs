//! Lightweight per-phase timing for `create_action` / `process_action`.
//!
//! Emits `BENCH <op>.<phase>: <ms> ms` lines via `console_log!`. Captured via
//! `wrangler tail` for benchmark analysis. No allocations beyond the static
//! &str names; cost per `lap()` is one `js_sys::Date::now()` call (~µs).
//!
//! Usage:
//! ```ignore
//! let mut t = BenchTimer::new("create_action");
//! t.lap("setup");
//! // ... work ...
//! t.lap("allocate_inputs");
//! // ...
//! t.done();
//! ```
//!
//! `done()` is consumed at end-of-fn and emits a `<op>.total: <ms>` line.

use worker::console_log;

pub struct BenchTimer {
    op: &'static str,
    start: f64,
    last: f64,
}

impl BenchTimer {
    pub fn new(op: &'static str) -> Self {
        let now = js_sys::Date::now();
        console_log!("BENCH {}.start", op);
        Self {
            op,
            start: now,
            last: now,
        }
    }

    /// Mark a phase boundary. Logs the delta since the last `lap()` (or `new()`).
    pub fn lap(&mut self, phase: &str) {
        let now = js_sys::Date::now();
        let delta = now - self.last;
        console_log!("BENCH {}.{}: {:.0} ms", self.op, phase, delta);
        self.last = now;
    }

    /// Mark a phase with an integer count attached (e.g. number of inputs).
    pub fn lap_with(&mut self, phase: &str, count: usize) {
        let now = js_sys::Date::now();
        let delta = now - self.last;
        console_log!("BENCH {}.{}[n={}]: {:.0} ms", self.op, phase, count, delta);
        self.last = now;
    }

    /// Final marker. Consumes self, emits total elapsed.
    pub fn done(self) {
        let now = js_sys::Date::now();
        let total = now - self.start;
        console_log!("BENCH {}.total: {:.0} ms", self.op, total);
    }
}

/// Inline timer for a single closure. Returns whatever the closure returns;
/// emits one BENCH line for the duration.
pub async fn timed<F, T>(op: &'static str, phase: &str, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let start = js_sys::Date::now();
    let result = f.await;
    let delta = js_sys::Date::now() - start;
    console_log!("BENCH {}.{}: {:.0} ms", op, phase, delta);
    result
}
