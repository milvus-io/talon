use super::*;

struct Pending<M: Machine> {
    request: Request<M>,
    lsn: u64,
}
struct Durable {
    done: oneshot::Sender<io::Result<Lsn>>,
    lsn: Lsn,
    _permits: Permits,
}
type SyncBatch = LocalBoxFuture<'static, (Vec<Durable>, io::Result<()>)>;
type WriteBatch<M> = LocalBoxFuture<'static, (Vec<Pending<M>>, io::Result<()>)>;

pub(crate) async fn run<M: Machine>(
    mut disk: disk::Disk,
    mut machine: M,
    mut receive: mpsc::Receiver<Request<M>>,
    failed: &AtomicBool,
    policy: Arc<std::sync::RwLock<Option<Arc<dyn Maintenance>>>>,
    snapshot_slots: Arc<Semaphore>,
) -> io::Result<()> {
    let mut write: Option<WriteBatch<M>> = None;
    let mut sync: Option<SyncBatch> = None;
    let mut durable = Vec::new();
    let mut barrier: Option<Request<M>> = None;
    let mut checkpoint: Option<LocalBoxFuture<'static, io::Result<disk::Snapshot>>> = None;
    let mut checkpoint_request: Option<Request<M>> = None;
    let mut closed = false;
    let mut next_lsn = disk.lsn;
    let result=async {
        loop {
            if failed.load(Ordering::Acquire) {return Err(stopped());}
            if sync.is_none() && !durable.is_empty() {
                let requests=std::mem::take(&mut durable);let sync_io=disk.sync();
                sync=Some(Box::pin(async move {(requests,sync_io.await)}));
            }
            if write.is_none() && sync.is_none() {
                if matches!(barrier.as_ref().map(|r|&r.command),Some(Command::Query(_))) {
                    disk.sync().await?;
                    let request=barrier.take().unwrap();
                    if let Command::Query(query)=request.command {query(&machine);}
                } else if checkpoint.is_none() && (barrier.is_some() || disk.offset>=disk.options.checkpoint_bytes) {
                    let snapshot=machine.snapshot()?;
                    let generation=disk.rotate().await?;
                    checkpoint=Some(disk.snapshot::<M>(generation,disk.lsn,snapshot,policy.read().unwrap().clone(),snapshot_slots.clone()));
                    checkpoint_request=barrier.take();
                }
            }
            if closed && write.is_none() && sync.is_none() && checkpoint.is_none() && barrier.is_none() {return Ok(());}
            tokio::select! {
                result=async {sync.as_mut().unwrap().await}, if sync.is_some()=>{
                    sync=None;let (requests,result)=result;result?;
                    for pending in requests {let _=pending.done.send(Ok(pending.lsn));}
                }
                result=async {checkpoint.as_mut().unwrap().await}, if checkpoint.is_some()=>{
                    checkpoint=None;disk.publish_snapshot(result?).await?;
                    if let Some(request)=checkpoint_request.take() {if let Command::Checkpoint(done)=request.command {let _=done.send(Ok(()));}}
                }
                result=async {write.as_mut().unwrap().await}, if write.is_some()=>{
                    write=None;
                    let (requests,result)=result;result?;
                    for pending in requests {
                        let Request{command,_permits}=pending.request;
                        if let Command::Append{record,bytes,ack,done}=command {
                            machine.apply(pending.lsn,record)?;disk.lsn=pending.lsn;
                            if ack==Ack::Durable {
                                // The applied record is no longer needed. Keep its
                                // admission and acknowledgement until the covering sync.
                                durable.push(Durable{lsn:pending.lsn,done,_permits});
                            } else {let _=done.send(Ok(pending.lsn));}
                            drop(bytes);
                        }
                    }
                }
                // Do not extend the log beyond an unfinished batch. Ordering
                // only apply/ack lets later pwrite calls leave an interior hole
                // after process loss, which is not a recoverable torn tail.
                request=receive.recv(), if !closed && write.is_none() && barrier.is_none() && (disk.offset<disk.options.checkpoint_bytes || checkpoint.is_some())=>{
                    let Some(request)=request else {closed=true;continue;};
                    if !matches!(request.command,Command::Append{..}) {barrier=Some(request);continue;}
                    let mut requests=vec![request];let mut bytes=Vec::new();let mut pending=Vec::new();
                    loop {
                        let request=requests.pop().unwrap();
                        let Command::Append{bytes:body,..}=&request.command else {unreachable!()};
                        next_lsn=next_lsn.checked_add(1).ok_or_else(||invalid("journal LSN overflow"))?;
                        bytes.extend_from_slice(&frame::encode(next_lsn,body)?);pending.push(Pending{request,lsn:next_lsn});
                        if pending.len()>=256 || bytes.len()>=256<<10 {break;}
                        match receive.try_recv() {Ok(r) if matches!(r.command,Command::Append{..})=>requests.push(r),Ok(r)=>{barrier=Some(r);break;},Err(_)=>break}
                    }
                    let offset=disk.offset;disk.offset=disk.offset.checked_add(bytes.len() as u64).ok_or_else(||invalid("journal offset overflow"))?;
                    let write_io=disk.write(bytes.into(),offset);
                    write=Some(Box::pin(async move {(pending,write_io.await)}));
                }
            }
        }
    }.await;
    // Accepted native I/O retains its buffers/descriptors and lease even after
    // another operation failed. Do not race a reopen against abandoned writes.
    if let Some(write) = write {
        let _ = write.await;
    }
    if let Some(sync) = sync {
        let _ = sync.await;
    }
    if let Some(checkpoint) = checkpoint {
        let _ = checkpoint.await;
    }
    result
}
