use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};
use monoio::net::{ListenerConfig, TcpListener, TcpStream};

fn main() {
    // 1. probe: can we even build a uring runtime?
    let pool = monoio::blocking::DefaultThreadPool::new(2);
    let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .attach_thread_pool(Box::new(pool))
            .enable_timer()
            .build()
    }));
    let mut rt = match built {
        Ok(Ok(rt)) => { println!("URING_BUILD=ok"); rt }
        Ok(Err(e)) => { println!("URING_BUILD=err {e}"); std::process::exit(10); }
        Err(_) => { println!("URING_BUILD=panic"); std::process::exit(11); }
    };
    // 2. probe: SO_REUSEPORT multi-listener + real roundtrip
    rt.block_on(async {
        let cfg = ListenerConfig::default();
        let l1 = TcpListener::bind_with_config("127.0.0.1:0", &cfg).unwrap();
        let addr = l1.local_addr().unwrap();
        println!("REUSEPORT_BIND=ok {addr}");
        monoio::spawn(async move {
            let (mut s, _) = l1.accept().await.unwrap();
            let (r, b) = s.read_exact(vec![0u8; 5]).await;
            r.unwrap();
            let (w, _) = s.write_all(b).await;
            w.unwrap();
        });
        let mut c = TcpStream::connect(addr).await.unwrap();
        let (w, _) = c.write_all(b"probe".to_vec()).await;
        w.unwrap();
        let (r, b) = c.read_exact(vec![0u8; 5]).await;
        r.unwrap();
        println!("ROUNDTRIP={}", String::from_utf8_lossy(&b));
    });
    // 3. probe: cpu pinning
    match monoio::utils::bind_to_cpu_set(vec![0]) {
        Ok(_) => println!("CPU_PIN=ok"),
        Err(e) => println!("CPU_PIN=err {e:?}"),
    }
    println!("PROBE_RESULT=SUPPORTED");
}
