//! WAL group commit (ADR 0003 §9.4).
//!
//! > One WAL writer per cache directory performs group commit. It issues an
//! > `fsync` when either 2 milliseconds have elapsed since the first unflushed
//! > record or 1 MiB of WAL records are waiting.
//!
//! One writer per *cache directory*, not per shard. Batching across shards is
//! the point: a per-shard writer would issue many more fsyncs for the same
//! throughput, and fsync cost is per call far more than per byte.
//!
//! # The constraint that shapes everything here
//!
//! > A caller is acknowledged only by the replicas whose required record is
//! > included in the completed `fsync`; **the timer is a maximum batching delay,
//! > not an early-acknowledgement path.**
//!
//! The obvious implementation — wake every waiter when the timer fires — would
//! acknowledge writes whose bytes are still in the page cache. That is the
//! failure the entire durability chain exists to prevent: the client is told the
//! write is safe, the machine loses power, and the write is gone.
//!
//! So a waiter is released by an fsync that *provably covered its record*, and
//! the timer only decides *when* to start one. [`Batcher`] therefore tracks a
//! durable watermark rather than a flag, and a record arriving mid-flush waits
//! for the next fsync rather than riding the one already in progress.

use core::time::Duration;

/// Bytes of pending records that trigger a flush.
pub const FLUSH_BYTES: usize = 1024 * 1024;

/// Time since the first unflushed record that triggers a flush.
///
/// Measured from the *first* pending record, not the most recent. A window that
/// restarted on every arrival would never close under sustained load, which is
/// exactly when bounded latency matters.
pub const FLUSH_INTERVAL: Duration = Duration::from_millis(2);

/// A position in the WAL, monotonically increasing with bytes appended.
///
/// Records are durable up to some sequence; a waiter is released when the
/// durable watermark reaches or passes its own position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct WalPosition(u64);

impl WalPosition {
    /// The position before anything is written.
    pub const START: Self = Self(0);

    /// Construct from a raw byte offset.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// The raw byte offset.
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Why the batcher decided to flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushTrigger {
    /// Pending bytes reached [`FLUSH_BYTES`].
    ByteThreshold,
    /// [`FLUSH_INTERVAL`] elapsed since the first unflushed record.
    Deadline,
    /// A caller asked for an immediate flush.
    Explicit,
}

/// Accumulates records and decides when to fsync.
///
/// Deliberately holds no file handle and calls no syscall: the caller performs
/// the write and the fsync and reports back. That keeps the policy — which is
/// where the durability argument lives — testable without a disk, and keeps this
/// type from having an opinion about io_uring versus blocking IO.
#[derive(Debug)]
pub struct Batcher {
    /// Bytes appended so far, flushed or not.
    appended: WalPosition,
    /// Bytes proven durable by a completed fsync.
    durable: WalPosition,
    /// Position captured when the in-flight fsync began, if one is running.
    ///
    /// A record appended after this point is *not* covered by that fsync, which
    /// is what stops the timer from becoming an early-acknowledgement path.
    in_flight: Option<WalPosition>,
    /// Elapsed time since the first record of the current batch.
    since_first_pending: Duration,
    /// Whether anything is pending.
    has_pending: bool,
}

impl Batcher {
    /// A batcher with nothing pending.
    pub fn new() -> Self {
        Self {
            appended: WalPosition::START,
            durable: WalPosition::START,
            in_flight: None,
            since_first_pending: Duration::ZERO,
            has_pending: false,
        }
    }

    /// Record that `bytes` were appended, returning the record's end position.
    ///
    /// A caller waits until [`durable`](Self::durable) reaches this value.
    pub fn append(&mut self, bytes: usize) -> WalPosition {
        self.appended = WalPosition::new(self.appended.get() + bytes as u64);
        if !self.has_pending {
            self.has_pending = true;
            self.since_first_pending = Duration::ZERO;
        }
        self.appended
    }

    /// Advance the clock, returning a trigger if a flush should start now.
    ///
    /// Returns `None` while a flush is in flight: fsyncs are serialised per
    /// writer, and starting a second would not make the first cover more.
    pub fn tick(&mut self, elapsed: Duration) -> Option<FlushTrigger> {
        if self.has_pending {
            self.since_first_pending += elapsed;
        }
        self.should_flush()
    }

    /// Whether a flush should start, without advancing the clock.
    pub fn should_flush(&self) -> Option<FlushTrigger> {
        if self.in_flight.is_some() || !self.has_pending {
            return None;
        }
        if self.pending_bytes() >= FLUSH_BYTES as u64 {
            return Some(FlushTrigger::ByteThreshold);
        }
        if self.since_first_pending >= FLUSH_INTERVAL {
            return Some(FlushTrigger::Deadline);
        }
        None
    }

    /// Begin a flush, returning the position it will make durable.
    ///
    /// Everything appended up to here is covered; anything appended after is
    /// not, and waits for the next flush.
    pub fn begin_flush(&mut self) -> WalPosition {
        let covered = self.appended;
        self.in_flight = Some(covered);
        // The next batch's deadline starts now, not when its first record
        // arrives during the flush. Otherwise a record appended mid-flush would
        // start its window only at completion and see a longer delay than the
        // bound promises.
        self.since_first_pending = Duration::ZERO;
        self.has_pending = false;
        covered
    }

    /// Report that the in-flight fsync completed.
    ///
    /// Advances the durable watermark to what that fsync covered — and no
    /// further, even if more was appended meanwhile.
    ///
    /// # Panics
    ///
    /// Panics if no flush was in flight. A completion without a start means the
    /// caller's bookkeeping is wrong, and continuing would advance the durable
    /// watermark past what was actually synced.
    pub fn complete_flush(&mut self) -> WalPosition {
        let covered = self
            .in_flight
            .take()
            .expect("complete_flush without begin_flush");
        self.durable = covered;
        // A record appended during the flush is still pending afterwards.
        if self.appended > covered {
            self.has_pending = true;
        }
        self.durable
    }

    /// Position proven durable by a completed fsync.
    pub fn durable(&self) -> WalPosition {
        self.durable
    }

    /// Whether a record ending at `position` may be acknowledged.
    ///
    /// The whole point of the type: this is true only when a completed fsync
    /// covered the record. There is deliberately no way to ask "has the timer
    /// fired", because that question must not influence an acknowledgement.
    pub fn is_durable(&self, position: WalPosition) -> bool {
        self.durable >= position
    }

    /// Bytes appended but not yet proven durable.
    pub fn pending_bytes(&self) -> u64 {
        self.appended.get() - self.durable.get()
    }

    /// Whether a flush is currently in flight.
    pub fn is_flushing(&self) -> bool {
        self.in_flight.is_some()
    }
}

impl Default for Batcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_full_batch_flushes_before_the_deadline() {
        let mut batcher = Batcher::new();
        batcher.append(FLUSH_BYTES);
        assert_eq!(
            batcher.tick(Duration::from_micros(1)),
            Some(FlushTrigger::ByteThreshold),
            "1 MiB pending must not wait for the 2ms timer"
        );
    }

    #[test]
    fn the_deadline_is_measured_from_the_first_pending_record() {
        // A window that restarted on every arrival would never close under
        // sustained load -- exactly when a latency bound matters.
        let mut batcher = Batcher::new();
        batcher.append(64);
        assert_eq!(batcher.tick(Duration::from_millis(1)), None);
        batcher.append(64);
        assert_eq!(
            batcher.tick(Duration::from_millis(1)),
            Some(FlushTrigger::Deadline),
            "the second record must not restart the window"
        );
    }

    #[test]
    fn nothing_pending_never_flushes() {
        let mut batcher = Batcher::new();
        assert_eq!(batcher.tick(Duration::from_secs(1)), None);
    }

    #[test]
    fn the_timer_alone_does_not_make_a_record_durable() {
        // The load-bearing assertion of §9.4: "the timer is a maximum batching
        // delay, not an early-acknowledgement path". If the deadline elapsing
        // were enough, a caller would be told its write is safe while the bytes
        // are still in the page cache.
        let mut batcher = Batcher::new();
        let position = batcher.append(64);
        assert_eq!(
            batcher.tick(Duration::from_millis(10)),
            Some(FlushTrigger::Deadline)
        );
        assert!(
            !batcher.is_durable(position),
            "an elapsed timer must not acknowledge anything"
        );

        batcher.begin_flush();
        assert!(
            !batcher.is_durable(position),
            "a started fsync must not acknowledge anything either"
        );

        batcher.complete_flush();
        assert!(
            batcher.is_durable(position),
            "only a completed fsync acknowledges"
        );
    }

    #[test]
    fn a_record_appended_during_a_flush_waits_for_the_next_one() {
        // The subtle case. The in-flight fsync was issued against a file that
        // did not contain this record, so completing it says nothing about the
        // record's durability. Releasing it here would be the same bug as
        // releasing on the timer, just harder to see.
        let mut batcher = Batcher::new();
        let first = batcher.append(64);
        batcher.begin_flush();
        let second = batcher.append(64);

        batcher.complete_flush();
        assert!(batcher.is_durable(first));
        assert!(
            !batcher.is_durable(second),
            "a record appended mid-flush is not covered by that flush"
        );

        batcher.begin_flush();
        batcher.complete_flush();
        assert!(batcher.is_durable(second));
    }

    #[test]
    fn a_second_flush_does_not_start_while_one_is_in_flight() {
        // fsyncs serialise per writer; a second would not make the first cover
        // more, and would complicate the watermark for nothing.
        let mut batcher = Batcher::new();
        batcher.append(FLUSH_BYTES * 2);
        batcher.begin_flush();
        assert_eq!(batcher.tick(Duration::from_millis(10)), None);
        assert!(batcher.is_flushing());
    }

    #[test]
    fn no_second_flush_starts_while_one_is_in_flight() {
        // Found by mutation testing: dropping the in-flight guard from
        // should_flush passed every test, because they asserted only that no
        // trigger was returned in a case where nothing was pending anyway.
        //
        // It is a real bug rather than an equivalent mutant. A second
        // begin_flush overwrites `in_flight`, so when the *first* fsync
        // completes it advances the durable watermark to the second flush's
        // position -- acknowledging bytes that fsync never wrote. This asserts
        // the guard holds with both thresholds far exceeded, which is when a
        // missing guard would actually fire.
        let mut batcher = Batcher::new();
        batcher.append(FLUSH_BYTES * 4);
        batcher.begin_flush();
        batcher.append(FLUSH_BYTES * 4);
        assert!(batcher.is_flushing());
        assert_eq!(
            batcher.tick(FLUSH_INTERVAL * 100),
            None,
            "no second flush may start while one is in flight"
        );
    }

    #[test]
    fn the_deadline_restarts_when_a_flush_begins() {
        // Otherwise a record appended during a flush would start its window
        // only at completion and could see a delay longer than the bound.
        let mut batcher = Batcher::new();
        batcher.append(64);
        batcher.tick(Duration::from_millis(5));
        batcher.begin_flush();
        batcher.append(64);
        batcher.complete_flush();

        assert_eq!(
            batcher.should_flush(),
            None,
            "the new batch's window starts fresh, not already expired"
        );
        assert_eq!(batcher.tick(FLUSH_INTERVAL), Some(FlushTrigger::Deadline));
    }

    #[test]
    fn pending_bytes_reflect_what_is_not_yet_durable() {
        let mut batcher = Batcher::new();
        batcher.append(100);
        assert_eq!(batcher.pending_bytes(), 100);
        batcher.begin_flush();
        batcher.append(50);
        assert_eq!(batcher.pending_bytes(), 150);
        batcher.complete_flush();
        assert_eq!(
            batcher.pending_bytes(),
            50,
            "only the flushed prefix stops being pending"
        );
    }

    #[test]
    fn the_durable_watermark_never_exceeds_what_was_synced() {
        // Guards against advancing to `appended` on completion, which would
        // acknowledge records the fsync never saw -- the same class of bug as
        // releasing on the timer.
        let mut batcher = Batcher::new();
        batcher.append(64);
        batcher.begin_flush();
        batcher.append(1_000_000);
        let durable = batcher.complete_flush();
        assert_eq!(durable, WalPosition::new(64));
        assert!(batcher.durable() < WalPosition::new(1_000_064));
    }

    #[test]
    #[should_panic(expected = "complete_flush without begin_flush")]
    fn completing_a_flush_that_never_started_panics() {
        // Silently ignoring it would leave the durable watermark behind while
        // the caller believes it advanced, and every later acknowledgement
        // would be wrong in a way nothing detects.
        Batcher::new().complete_flush();
    }

    #[test]
    fn the_thresholds_match_the_adr() {
        assert_eq!(FLUSH_BYTES, 1024 * 1024);
        assert_eq!(FLUSH_INTERVAL, Duration::from_millis(2));
    }
}
