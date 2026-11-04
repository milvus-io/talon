use super::*;

#[cfg(test)]
mod tests;

const FLUSH_INTERVAL: Duration = Duration::from_secs(30);
const FLUSH_BYTES: u64 = 8 << 20;

impl SegmentStore {
    pub(super) async fn flush_due(&self) -> bool {
        let state = self.state.lock().await;
        state
            .active
            .and_then(|id| state.segments.get(&id))
            .is_some_and(|s| {
                s.bytes > s.durable_end
                    && (state.last_flush.elapsed() >= FLUSH_INTERVAL
                        || s.bytes - s.durable_end >= FLUSH_BYTES)
            })
    }

    // The caller owns this operation through Mutations. Publication excludes
    // deletes, seal and relocation while appends beyond the captured cut continue.
    pub(super) async fn flush(&self) -> io::Result<()> {
        let _publication = self.publication.lock().await;
        let (id, end) = {
            let state = self.state.lock().await;
            self.healthy(&state)?;
            let Some(id) = state.active else {
                return Ok(());
            };
            let segment = &state.segments[&id];
            if segment.bytes == segment.durable_end {
                return Ok(());
            }
            (id, segment.bytes)
        };
        #[cfg(test)]
        {
            let pause = self.flush_pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause.entered.notify_one();
                pause.resume.notified().await;
            }
        }
        let result = async {
            let data = loop {
                let drained = self.appends_drained.notified();
                tokio::pin!(drained);
                drained.as_mut().enable();
                let state = self.state.lock().await;
                self.healthy(&state)?;
                let segment = &state.segments[&id];
                if segment.in_flight.range(..end).next().is_none() {
                    break segment.data().await?;
                }
                drop(state);
                drained.await;
            };
            disk_io::sync(data).await?;
            #[cfg(test)]
            super::tests::crash("flush_data");
            self.sync_data_directory(id).await?;
            #[cfg(test)]
            super::tests::crash("flush_directory");
            self.journal
                .append(
                    journal::Event::Flush {
                        segment: id,
                        durable_end: end,
                    },
                    true,
                )
                .await?;
            #[cfg(test)]
            super::tests::crash("flush_commit");
            Ok(())
        }
        .await;
        let mut state = self.state.lock().await;
        match result {
            Ok(()) => {
                self.healthy(&state)?;
                state.segments.get_mut(&id).unwrap().durable_end = end;
                state.last_flush = Instant::now();
                Ok(())
            }
            Err(error) => {
                state.poisoned = true;
                self.pipeline_changed.notify_waiters();
                self.appends_drained.notify_waiters();
                Err(error)
            }
        }
    }
}
