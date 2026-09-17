# Page idle TTL

Worker 的 paged L2 缓存可以按最后访问时间回收。配置默认关闭；TTL 是本地缓存策略，
不会删除源对象，不涉及 write-back staging、WAL 或跨 Worker 副本元数据。

## 使用方式

```toml
l2_page_size_bytes = 1048576
page_ttl_ms = 86400000
page_access_checkpoint_interval_ms = 60000
page_gc_interval_ms = 1000
page_gc_scan_batch_size = 65536
page_gc_delete_batch_size = 1024
page_gc_io_concurrency = 4
```

24 小时仅为示例。环境变量为上述字段加 `TALON_WORKER_` 前缀并转为大写，
例如 `TALON_WORKER_PAGE_TTL_MS=86400000`。环境变量覆盖 TOML；没有专用 CLI flag 或热更新。
完整默认值见[配置参考](../reference/configuration.md)。

`page_ttl_ms=0` 关闭 TTL 和访问时间 checkpoint，保留容量淘汰。
开启 TTL 必须启用 paged L2，checkpoint 周期不能超过 TTL；其他周期、批量和并发配置必须为正数。
`capacity_bytes=0` 只关闭容量淘汰，不关闭 TTL。

## 访问与回收语义

只有 `now - last_access > TTL` 才能因 TTL 回收，等于 TTL 时保留。
L1 命中、L2 成功读取、sendfile 成功获取覆盖范围的 FD 和新 page 成功提交会更新时间。
多 page 读取只更新涉及的 page；HEAD/stat、注册、心跳、扫描和 checkpoint 不续期。

超过 TTL 但未被 GC 认领的 page 仍可命中并续期。GC 认领后不能再获取新的缓存读取资源，
普通读取走已有 miss/回源路径，cache-only 读取保持原有 cache miss 语义。
公共客户端和 wire protocol 不变。

进程内时间从启动时的 Unix 时间锚点加单调时钟经过时长得到，避免运行中的时钟回拨。
重启时使用当前 Unix 时间恢复年龄，停机时间计入 TTL。记录缺失、损坏或位于未来时，
page 是可回收候选，实际访问仍可在认领前续期。跨重启时钟异常可能导致提前回收。

TTL 是异步回收条件，不是物理空间释放期限。GC 扫描/删除预算、失败重试及在途 FD
都会影响空间释放延迟。容量淘汰可以在 TTL 到期前回收 page。

## 模块与并发协议

| 模块 | 职责 |
| --- | --- |
| `page_lifecycle` | 分片 block registry、page 时间/代次/修订号、RAII 读取保护、可恢复扫描游标 |
| `page_access_store` | `access.meta` 编解码、校验、原子替换、恢复及 cache root 独占锁 |
| `page_cleanup` | 物理目录游标扫描、崩溃临时文件和无 page 目录的限量清理 |
| `page_gc` | 配置、GC/checkpoint 调度、任务所有权与退出、指标 |
| `runtime/page_maintenance` | 配置恢复、`gc_once`、checkpoint 和统一 page 删除协议 |
| `runtime` | L1/L2/sendfile/cache-only 接入，提交事务及容量/旧版本淘汰 |
| `eviction` | 非破坏性候选选择、成功 unlink 后记账、RAII 容量 pin |
| `paged_store` / `index` / `memory_store` | 文件删除、FD 失效及成功后的 residency 更新 |
| Worker `main` / `uring_serve` | 启动恢复、后台任务、SIGINT/SIGTERM 及 ring 停止 |

page 状态是 `Resident -> Evicting -> Absent`；unlink 失败恢复 `Resident`，保留记账并重试。
候选携带驻留代次和访问修订号，实际认领重新检查这些字段、TTL 和读取保护。
读取先获得 guard 时 GC 跳过；GC 先认领时后续读取 miss。

同一 block 的提交、删除、checkpoint 和空目录清理共用 mutation gate。
不同 block 独立执行；后端 fetch 在 gate 外。每次只获取一个 block gate，
内存状态锁不跨 I/O 或 await。容量和旧版本 page 淘汰复用 TTL 的删除协议。
磁盘残留清理按物理目录摘要协调，不要求能解析 `block.meta`。固定 256 个目录锁分片
由提交、删除和 checkpoint 以共享模式持有，再获取 block gate；清理以独占模式持有。
不同 block 的正常 mutation 可以并行，清理会短暂阻塞同一分片的 mutation。
容量候选在删除前复查访问修订号、pin 和当前容量压力，whole-block 也执行这些检查。
候选被保护或删除失败时，从其他缓存单元补选；每轮最多尝试开始时的缓存单元数量，
同一单元本轮只尝试一次，避免持续失败或并发提交让容量治理无限循环。

删除顺序为认领、FD cache 失效、unlink、L1/bitmap/LRU/时间条目更新、空目录清理。
unlink 成功或 NotFound 才扣除驻留字节；失败保留重试状态，重试前再次检查新访问。
空目录清理失败会由后续扫描重试。元数据扫描使用稳定 block 槽位和有序 page key，
每批检查工作受预算限制，不一次复制全局 page 列表。
扫描达到删除预算时也停止并保留游标，不丢弃尚未尝试的候选，避免失败重试长期阻塞后续 page。

读取 guard 保持到 bytes 或 FD 已安全取得；之后无需让慢客户端阻塞 TTL。
已打开的 FD 可以在 unlink 后完成读取，最后一个 FD 关闭后内核才释放文件占用。
提交和删除由独立的 Worker 任务持有，调用者取消不会在 blocking I/O 仍执行时释放 gate。
提交后的容量及旧版本清理也由 Worker 任务持有，在释放 block gate 后继续完成。

当完整 block admission 与已有同版本 paged 缓存相遇时，补齐 paged 内容而不切换物理形态，
避免与 page GC/在途读取产生两套 residency 记账。

## 访问时间文件

```text
<block>.pages/
  block.meta
  0.page
  1.page
  access.meta
```

现有 page 与 `block.meta` 格式不变。`access.meta` 是独立的小端二进制文件：

| 字段 | 编码 |
| --- | --- |
| magic | 8 字节 `TLNACC01` |
| version | u32，当前为 1 |
| BlockId | u32 长度及 JSON 编码的完整现有 BlockId |
| page size | u32 |
| revision / sampled Unix ms | 两个 u64 |
| record count | u32 |
| records | 按 page index 排序，每条 `u32 index + u64 last_access_ms` |
| checksum | 前述所有字节的 XXH3-64，u64 |

恢复校验身份、page size、文件长度、排序/重复索引和校验和，不接受部分损坏文件。
page 文件扫描才是驻留事实来源：没有文件的时间记录被忽略，没有有效时间的文件可回收。

访问只修改内存。每个 block 只有一份 dirty 状态；每轮分批遍历 dirty block，
最多两个 checkpoint 并发。复制时间与 revision 后释放状态锁，再在 blocking pool
写临时文件、同步文件、原子替换、同步父目录。期间的新访问推进 revision，
因此不能被旧 checkpoint 的完成状态误标为已保存。失败保留 dirty 状态和可用的已有文件。

保存周期不是误差上限。失败或积压会增加未保存窗口；一次丢失的访问就可能让
恢复的时间比实际时间老很多。允许这类提前回收，后续访问重新回源。
不存在异常重启后给所有 page 延长一个 TTL 的行为。

## 崩溃残留兜底

正式 `access.meta` 通过原子替换更新，不保留历史版本。异常退出留下的临时文件，
以及最后一个 page 删除后尚未清理的目录，由独立磁盘扫描回收：

- 启动持有 cache root 独占锁，在接收请求前完成一轮限量分批扫描；不依赖 residency
  索引，因此无 page、缺少或损坏 `block.meta` 的目录仍可被发现。
- 后台按 `page_gc_interval_ms`（默认 1 秒）推进；每批使用 `page_gc_scan_batch_size`
  和 `page_gc_delete_batch_size` 的独立预算，并与 page GC 共享 I/O 并发限额。
  即使关闭 TTL 或切回 whole-block 读取模式，也继续扫描已有的 `paged/` 目录。
- 删除已识别的 `access.meta.tmp.*`，以及符合现有命名格式的 `block.meta.tmp.<pid>.<seq>`
  和 `<page>.page.tmp.<pid>.<seq>`。清理与在途提交/checkpoint 互斥，不按文件年龄猜测。
- 删除目录前重新检查：只允许剩余 `access.meta`、`block.meta` 两个普通文件或空目录。
  每次确认至多检查三个目录项，逐文件删除后使用非递归 `remove_dir`；存在 page、未知文件
  或符号链接时保留目录。目录读取错误不当作空目录。常规枚举预算外有这项常数开销。
- 扫描最多保留三级目录迭代器，不积累全局文件列表或无限重试队列。失败后推进其他目录，
  下一轮从磁盘重新发现；重启也能恢复清理。删除预算为 1 时，metadata 和目录可跨批处理，
  每次继续前都重新检查是否已有新的 page。

这保证文件系统恢复可操作且后台持续运行后，已识别残留能够最终清理；永久 I/O 错误
或未知文件需要按告警排查。清理过程不改变有效 page 的访问时间或 TTL。

## 重启、failover、升级

- cache root 持有进程级独占锁。部署仍必须隔离旧版本实例，因为旧版不识别此锁。
- 启动先扫描 page、恢复访问时间和记账，再启动服务及后台任务。过期数据限速清理，
  不等待清空才接收请求。临时访问文件不作为有效 checkpoint；上述残留扫描会在启动时
  尝试一整轮，失败的路径留给后台重试。
- 正常退出停止新后台调度，等待已有 mutation，尽力做最终 checkpoint，整体预算 10 秒。
  最终保存不保证覆盖并发尾部请求；超时或 SIGKILL 都按最近有效 checkpoint 恢复。
- 同磁盘重启/failback 保留已保存年龄，incarnation、注册和 placement 变化不续期。
- 新磁盘/其他 Worker 使用自己的缓存状态，miss 按现有路径回源；没有访问时间广播。
- Coordinator 断连或后端暂不可用不暂停 TTL。后端不可用时，失去的缓存无法保证立即回填。
- 初次开启时旧 page 没有访问记录，可能集中回收。按 Worker 小批开启并观察回源压力。
- 回滚旧版后不执行 TTL；再次升级不补偿旧版或 TTL 关闭期间未保存的访问。
- 当前服务使用首个 cache dir，本功能不新增多磁盘调度。

<a id="diagnostics"></a>
## 监控与排障

所有新指标以 `talon_worker_page_` 开头，无 object/page 标签。

- `gc_scanned_total`、`gc_batch_seconds`、`gc_scan_seconds`：扫描进度与耗时。
- `gc_reclaimed_total`、`gc_reclaimed_bytes_total`：成功回收；`reason=ttl|capacity|superseded`。
- `gc_delete_errors_total`、`gc_pending_retries`：删除故障和最近完整扫描看到的重试量。
- `access_dirty_blocks`、`access_oldest_dirty_seconds`：checkpoint 遍历观察到的未保存状态。
- `access_checkpoint_bytes_total`、`access_checkpoint_errors_total`、
  `access_checkpoint_timestamp_seconds`：保存流量、失败和最近成功时间。
- `access_recovery_missing_total`、`access_recovery_corrupt_total`、
  `access_recovery_future_total`：恢复异常。
- `cleanup_scanned_total`、`cleanup_removed_total`、`cleanup_errors_total`：磁盘残留枚举、
  删除和错误计数。删除计数包含临时文件、孤儿 metadata 文件及空目录。
- `cleanup_pending`：最近完整扫描遇到清理错误的路径数量，属于扫描观察值。
- `cleanup_scan_seconds`、`cleanup_scan_timestamp_seconds`：完整磁盘扫描耗时及最近完成时间。

残留清理持续失败或 15 分钟未完成一轮扫描时告警，持续时间均为 5 分钟。
大容量缓存可能需要调整扫描预算或告警阈值；未知文件保留，需要人工确认后处理。

checkpoint 年龄超过三个周期、扫描一轮超过 TTL 的 10%、删除持续失败或重试增长时告警。
这些是后台遍历观测值，并非每次访问同步维护的精确全局快照。

保存故障先检查目录权限、磁盘空间、I/O 延迟及 fsync 错误；它不停止活进程的内存 TTL，
但会增加重启后的回源量。删除故障检查权限/文件系统错误，不把逻辑回收计数当作物理空闲空间。
比较文件系统用量与 resident bytes 时，需要考虑未关闭的 FD、元数据文件和 whole-block 缓存。

## 验证

可注入时钟测试覆盖边界、续期、丢失 checkpoint 的重启、cache-only、读取保护、
请求取消、删除失败重试、sendfile FD 生命周期、限量删除和并发 page 提交。
现有 Tokio/io_uring 数据面回归测试也必须通过。

```sh
cargo test -p talon-core -p talon-worker --lib --bins
cargo clippy -p talon-core -p talon-worker --all-targets -- -D warnings
cargo fmt --all --check
just check-config-docs
cargo test -p talon-worker --lib page_ttl_metadata_scale -- --ignored --nocapture
```

规模测试覆盖 10 万/100 万 page 的元数据访问、分批扫描和真实 checkpoint 文件写入。
它不生成对应数量的 page 数据文件，也不代表包含源存储、网络和数据页 I/O 的端到端性能。
生产上线仍需在实际 page size、磁盘、并发和热点分布下确认读 p99、回源压力与物理回收速度。
