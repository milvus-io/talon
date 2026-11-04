//! Startup-only CPU work. Never creates a pool for a small cache, and never
//! shares the foreground runtime or its I/O ring.

pub(crate) fn workers() -> usize {
    // Use the shared journal's CPU budget for the subsequent index stage too.
    // Keeping a second default here would let the two recovery phases drift.
    talon_journal::Options::default().recovery_threads
}

/// Build independent entries outside index locks, retaining input order. The
/// caller owns the input; this does not clone a second full set of metadata.
pub(crate) fn map<T: Send, U: Send>(
    input: Vec<T>,
    threads: usize,
    work: impl Fn(T) -> U + Sync,
) -> Vec<U> {
    let threads = threads.max(1).min(input.len().div_ceil(1024).max(1));
    if threads == 1 {
        return input.into_iter().map(work).collect();
    }
    std::thread::scope(|scope| {
        let size = input.len().div_ceil(threads);
        let mut input = input.into_iter();
        let mut tasks = Vec::new();
        for _ in 0..threads {
            let chunk: Vec<_> = input.by_ref().take(size).collect();
            let work = &work;
            tasks.push(scope.spawn(move || chunk.into_iter().map(work).collect::<Vec<_>>()));
        }
        tasks
            .into_iter()
            .flat_map(|t| t.join().expect("cache recovery task panicked"))
            .collect()
    })
}
