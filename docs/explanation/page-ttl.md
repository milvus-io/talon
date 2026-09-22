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

`page_ttl_ms=0` 关闭 TTL 和访问时间 checkpoint，保留容量淘汰。命中时不取时间、
不更新时间戳或 dirty 状态，仍保留读取/删除仲裁和容量策略的访问记录。
开启 TTL 必须启用 paged L2，checkpoint 周期不能超过 TTL；其他周期、批量和并发配置必须为正数。
`capacity_bytes=0` 只关闭容量淘汰，不关闭 TTL。

## 访问与回收语义

只有 `now - last_access > TTL` 才能因 TTL 回收，等于 TTL 时保留。
TTL 开启时，L1 命中、L2 成功读取、sendfile 成功获取覆盖范围的 FD 和新 page 成功提交会更新时间。
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
| `page_lifecycle` | 分片 block registry、稳定 page handle、原子读取保护与时间、可恢复扫描游标 |
| `page_access_store` / `page_access_shard` | 旧 block 元数据恢复、分片快照编解码与原子替换、cache root 独占锁 |
| `page_cleanup` | 物理目录游标扫描、崩溃临时文件和无 page 目录的限量清理 |
| `page_gc` | 配置、GC/checkpoint 调度、任务所有权与退出、指标 |
| `runtime/page_maintenance` | 配置恢复、`gc_once`、checkpoint 和统一 page 删除协议 |
| `runtime` | L1/L2/sendfile/cache-only 接入，提交事务及容量/旧版本淘汰 |
| `eviction` | 非破坏性候选选择、成功 unlink 后记账、RAII 容量 pin |
| `paged_store` / `index` / `memory_store` | 文件删除、FD 失效及成功后的 residency 更新 |
| Worker `main` / `uring_serve` | 启动恢复、后台任务、SIGINT/SIGTERM 及 ring 停止 |

page 状态是 `Resident -> Evicting -> Absent`；unlink 失败恢复 `Resident`，保留记账并重试。
候选携带 page handle 和选择序号。读取计数、删除认领、成功访问标记及失败重试原因
共用一个原子状态字；认领以 CAS 同时检查读取计数和访问标记，再复查 TTL。
读取先获得 guard 时 GC 跳过；GC 先认领时后续读取 miss。成功访问使旧候选失效，
即使时间戳仍处于同一毫秒。再次选择候选递增选择序号，不会让更早的候选重新有效。
扫描选择也用 CAS 确认状态未变，避免恢复已被并发访问取消的删除重试。
删除后旧 handle 永久关闭；重新提交同一 page 会创建新 handle。

同一 block 的提交、删除和空目录清理共用 mutation gate。
不同 block 独立执行；后端 fetch 在 gate 外。每次只获取一个 block gate，
内存状态锁不跨 I/O 或 await。容量和旧版本 page 淘汰复用 TTL 的删除协议。
磁盘残留清理按物理目录摘要协调，不要求能解析 `block.meta`。固定 256 个目录锁分片
由提交和删除以共享模式持有，再获取 block gate；清理以独占模式持有。
不同 block 的正常 mutation 可以并行，清理会短暂阻塞同一分片的 mutation。
容量候选在删除前复查策略句柄、pin 和当前容量压力；page 还通过上述认领协议
检查成功访问标记和读取保护，whole-block 保留现有策略检查。
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
paged/
  ab/                         # BlockId 哈希的前两个十六进制字符，最多 256 个分组
    access.shard              # 本分组所有驻留 block 的访问时间快照
    ab0123456789abcd.pages/
      block.meta
      0.page
      access.meta             # 升级前的旧文件；新版本不再写入
```

page 与 `block.meta` 格式不变。新快照使用 `TLNSHR01` magic、page size、block 数量、
长度前缀的 block 访问记录及整个文件的 XXH3-64 校验和。每条 block 记录沿用
`TLNACC01` version 1 编码：完整 BlockId、page size、revision、采样 Unix ms、
按 page index 排序的 `(u32 page, u64 last_access_ms)` 列表及独立校验和。
恢复拒绝错误分片、重复 block、重复/越界 page、格式错误和部分损坏的快照。
单分片快照上限为 64 MiB；超限保留 dirty 状态并报告 checkpoint 错误，不截断记录。

启动时逐分片恢复。**只有 `access.shard` 不存在时才读取旧 `access.meta`**；
新文件存在但损坏、缺少某条记录或包含未来时间时，不回退到旧文件。
首次开启 TTL 会将恢复得到的访问时间写入新格式，包含未发生新访问的 block。
旧文件保留到相应 block 目录被清理，因此迁移中断可重试，无需原地改写旧文件。
回退旧 Worker 会重新使用旧访问时间，可能提前淘汰；本地缓存可重新回源。
page 文件扫描始终是驻留事实来源：没有 page 文件的记录被忽略，没有有效时间的文件可回收。

访问只修改内存。索引的现有 page 策略条目同时持有稳定 page handle，L1/L2/sendfile
命中在同一次索引查询中取得读取保护，不再查询生命周期 registry 或 block 内的 page 表。
命中不获取 block 状态锁，不更新 block revision；revision 只跟踪提交和删除等结构变化。
单页命中不为 guard 分配数组；每个 guard 仍有一次 page Arc 克隆/释放，以及原子 pin/unpin。
成功访问标记、重试取消与 unpin 合并为一次 CAS。L2 字节读取保留 guard 到 L1 admission 完成。

TTL 开启时，每次成功读取仍取单调锚定时间；单次 sendfile 范围共用一个时间值。
时间戳只向前推进；同一毫秒或晚完成的较早读取跳过时间戳和 dirty 写入。
时间戳推进时只更新本 page 的原子 dirty 标记，不修改 block 共享状态。
这些是路径上的开销减少，不代表已通过性能验收。
后台按配置周期轮转 256 个分片，每个 tick 处理一个分片；tick 最小为 1 ms。
同一 Worker 同时只有一个 checkpoint，写入按 64 KiB 分块，限制为 8 MiB/s；
实际全轮耗时可能因 I/O、数据规模和调度超过配置周期。显式 flush 和关闭时遍历全部
分片，不等待后台 tick，但仍遵守字节速率限制。未变脏且成员未变化的分片不写盘。

每次只构造一个分片的快照，registry 遍历每批最多 64 个 block。重写分片时包含
其中未变脏的存量 block，避免覆盖丢失。小范围访问也可能重写整个分片；这是减少
逐 block fsync 次数与额外顺序写入之间的取舍。删除的条目会在后续快照中移除。
采样时先原子取走每个 page 的 dirty 标记，再读取时间戳，并记录 block 结构 revision。
随后发生的访问会留下新标记；保存失败或快照构造提前退出会恢复取走的标记。
成功保存只确认已采样的标记和结构 revision，不清除并发访问留下的新标记。
复制完成后释放状态锁，在 blocking pool 写临时文件、
同步文件、原子替换、同步父目录。checkpoint 不持有 block mutation gate，不阻塞
前台提交/删除；磁盘 I/O 期间的新访问、删除或新成员不能被误标为已保存。
调用者取消后，已接纳的 checkpoint 仍由 Worker 持有，完成发布及 dirty 状态更新。

保存周期不是误差上限。失败或积压会增加未保存窗口；一次丢失的访问就可能让
恢复的时间比实际时间老很多。允许这类提前回收，后续访问重新回源。
不存在异常重启后给所有 page 延长一个 TTL 的行为。

## 崩溃残留兜底

正式 `access.shard` 通过原子替换更新，不保留历史版本。异常退出留下的临时文件，
以及最后一个 page 删除后尚未清理的目录，由独立磁盘扫描回收：

- 启动持有 cache root 独占锁，在接收请求前完成一轮限量分批扫描；不依赖 residency
  索引，因此无 page、缺少或损坏 `block.meta` 的目录仍可被发现。
- 后台按 `page_gc_interval_ms`（默认 1 秒）推进；每批使用 `page_gc_scan_batch_size`
  和 `page_gc_delete_batch_size` 的独立预算，并与 page GC 共享 I/O 并发限额。
  即使关闭 TTL 或切回 whole-block 读取模式，也继续扫描已有的 `paged/` 目录。
- 分片目录中的 `access.shard.tmp.*` 使用独立 checkpoint gate 清理；在途写入、未知文件和符号链接保留。正式快照即使为空也保留，避免恢复时重新启用旧 block 文件。
- 删除已识别的旧 `access.meta.tmp.*`，以及符合现有命名格式的 `block.meta.tmp.<pid>.<seq>`
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
