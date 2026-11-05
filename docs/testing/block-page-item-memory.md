# PR #675：Paged store 的 block 与 page 内存拆解

前半部分保留优化前基线；第一轮实现后的结果见文末。

分析基线：`0205d79ab3287691c1623bc389a859744abed5da`；本地分支：
`analysis/pr675-block-page-memory`。测量日期：2026-11-05。

这里的 block 是开启 page 后管理一组 pages 的 block，page 是其中一个 resident page。
本报告按两者的字段和分配归属拆解，排除 WholeBlockStore 的整块存储模式。

在身份字符串合计 144 B、每个 block 属于不同 object、每个 block 只有一个 resident page
的恢复后状态下，默认维护配置的 **1,102.63 B 是整个 block 加其一个 page 的合计**：

```text
block 元数据             550.003 B
一个 page 的元数据       170.213 B
默认 GC 维护元数据       382.411 B
合计                   1102.627 B ≈ 1102.63 B
```

关闭 stale GC、ownership GC、TTI 时，合计为 **720.216 B**。
这些是存活堆分配的均摊请求字节，包含 Arc 控制块、锁、容器空槽和树节点，不是 RSS。

## 统计口径

- Linux x86_64，仓库固定 Rust 1.96.1，`wt-build` 开发容器，Cargo test profile。
- 普通 paged 文件缓存，不含 small store；L1 关闭。配置默认值以该提交的
  [WorkerConfig](../../crates/talon-core/src/config.rs) 为准。
- bucket/path/version 分别为 16/96/32 B，`L = 144 B`；每 object 一个 block、一个 version。
- 使用 16,384 个 block 分摊索引容量；catalog、index、policy 共享恢复出的 BlockKey。
  对象图通过真实 `restore_allocated`、`restore_policy_units`、`Lru::from_recovered` 构建。
- 排除临时恢复数组和 scope 固定分配：`Arc<BlockIndex>` 为 192 B，空 `PageLifecycle`
  的分片/锁等堆分配为 90,112 B。未构造完整 Worker、journal 服务或数据文件。
- 默认配置下 `malloc_usable_size` 合计约 1,160 B/block，仍不等于 RSS。
  分配器元数据、空闲 arena、线程栈及运行时另计；页缓存、inode/dentry 属于内核内存。

## Block 侧元数据

按字段归属，把内联的第一个 PageEntry 计入 page 侧：

| 结构 | B/block | 组成 |
| --- | ---: | --- |
| 共享身份 `Arc<Packed>` 与字符串 | 208 | Packed 48 + Arc 16 + 字符串 144 |
| Directory 哈希表 | 34.001 | BlockKey 指针、slot 编号、空槽及控制区域 |
| dense `DirectoryEntry` | 48 | resident 8 + lifecycle 8 + PolicyUnits 24 + activity 8 |
| `Arc<Entry>` | 88 | Arc 16 + RwLock 开销 16 + IndexedBlock 56 |
| PageTable 的非 PageEntry 部分 | 40 | Arc 16 + RwLock 开销 16 + 页索引/布局余量 8 |
| catalog `Arc<Block>` | 64 | Arc 16 + Block 48，包含内联页分配信息 |
| catalog 哈希表 | 34.001 | BlockKey、Arc<Block> 及哈希容量开销 |
| `VersionIndex.first` | 34.001 | logical digest → 共享 BlockKey |
| **Block 侧小计** | **550.003** | 未加默认维护元数据 |

**40 B 不是一次独立分配。** 实际 PageTable 分配是 136 B，包含上表的 40 B 和
下一节的 PageEntry 96 B。若按物理分配归属记账，block 关联分配为 **646.003 B**，
page 外置分配为 **74.213 B**，两种口径合计相同，不能重复计算 PageEntry。

`IndexedBlock` 的 56 B = ResidentMeta 24 + IndexedUnits 16 + charged_bytes 8 +
charged_pages 8。ResidentMeta 的 24 B = BlockKey 8 + len 8 + page_size 4 + page_count 4。
这是结构体本体；上表还包括它关联的身份、索引和容器内存。
PagedBlockStore 自身的 104 B 是共享 store 对象，不按 block 复制。

源码：[identity](../../crates/talon-worker/src/block_key.rs)、
[directory](../../crates/talon-worker/src/index/directory.rs)、
[index](../../crates/talon-worker/src/index.rs)、
[catalog](../../crates/talon-worker/src/catalog/model.rs)。

## Page 侧元数据

| 结构 | B/page | 组成 |
| --- | ---: | --- |
| `PageEntry` | 96 | UnitSlot 80 + handle 指针 8 + selection 8；单页时内联在 PageTable |
| `Arc<PageState>` | 48 | Arc 16 + PageState 32 |
| capacity LRU 队列项 | 26.213 | 恢复后批量构建的 BTreeMap 节点均摊 |
| **Page 侧小计** | **170.213** | 单页表示，包含内联 PageEntry |

`UnitSlot` 的 80 B = Option<AccessHandle> 32 + Option<PolicyState> 32 + allocated 8 +
resident 1 + padding 7。PolicyState 的 32 B = bytes/position/revision 各 8 + pins 4 +
independent 1 + padding 3；这些已包含在 96 B PageEntry 内。

PageState 32 B = `OnceLock<Box<PageAccess>>` 16 B + 两个 AtomicU64 共 16 B。
TTI 关闭时不分配 PageAccess。本恢复样本中，PageEntry.handle 与 AccessHandle
指向同一个 PageState；
index、policy、lifecycle 也共用同一张 PageTable，不能按三个模块重复计数。

**从一个 page 增加到两个 page，会从内联表示切换为 BTreeMap。**
原来的 136 B PageTable 仍然保留，同时新分配页表叶节点 **1,112 B**、
catalog 页分配表叶节点 **144 B**、第二个 PageState **48 B**，以及一个 LRU 队列项。
这使第二页的边际开销约为 **1,330 B**，高于单页 block 的均摊值。

| 每 block resident pages | 关闭三项维护：B/block | 均摊 B/page | 默认维护：B/block |
| ---: | ---: | ---: | ---: |
| 1 | 720.22 | 720.22 | 1,102.63 |
| 2 | 2,050.41 | 1,025.21 | 2,432.82 |
| 4 | 2,198.80 | 549.70 | 2,581.21 |
| 16 | 5,793.46 | 362.09 | 6,176.79 |
| 64 | 18,146.79 | 283.54 | 18,530.12 |
| 256 | 73,541.90 | 287.27 | 73,925.23 |

1/2/4 页采用 16,384 blocks，16/64/256 页采用 1,024 blocks；均排除 scope 固定开销。
多页案例按 page index 递增插入。BTreeMap 节点分裂及填充率使每页成本不严格单调。
页表内部节点为 **1,208 B**，catalog 内部节点为 **240 B**。
两者每个叶节点最多容纳 11 项；稀疏节点仍占完整节点空间。

因此 page 的预算应写成：**每 block 共享结构 + 两棵页树节点 + 每 page 的
48 B PageState + 每 page 一项 capacity queue**，不能直接用 `page_count × 720 B`。
这里 page_count 指实际 resident pages，逻辑上存在但未缓存的页不各分配一套元数据。
`PresentBitmap` 在当前 resident 表示中已删除，只在 API/恢复等临时表示中重建。

源码：[PageMap](../../crates/talon-worker/src/page_table.rs)、
[PageEntry/PageState](../../crates/talon-worker/src/page_lifecycle.rs)。

## 默认维护功能增加的内存

默认 stale GC 和 ownership GC 为 true，TTI 为 0。恢复时会开启 `track_checks` 和
`track_blocks`；以下按 block/object 计算，在前面的 720.216 B 之外增加。

| 结构 | 本例每 block 增加 |
| --- | ---: |
| `Box<BlockPolicy>` | 48 B |
| block 访问时钟 `Arc<AtomicU64>` | 24 B |
| block_queue + ownership_queue | 18.205 × 2 = 36.410 B |
| `object_checks` 哈希表 | 162.001 B |
| object_checks 独立 bucket/path 字符串 | 16 + 96 = 112 B |
| **合计** | **382.411 B** |

**720.216 + 382.411 = 1,102.627 B**。默认维护的 382.411 B 不在基础结构小计内。

object_checks 的内联键值为 `ObjectId` 56 B + `(usize, Option<u64>)` 24 B = 80 B。
它按 object 去重；若同一 object 有多个 blocks，274 B 的检查索引部分应在这些 blocks
之间分摊。BlockPolicy、时钟和两条 block 队列仍按 block 计数。
只关闭 stale GC 会去掉 object_checks，但 ownership GC 或 TTI 仍可保留 block tracking。

TTI 绑定另外增加的结构：

| 结构 | 每个 paged block / page 增加 |
| --- | ---: |
| `Arc<BlockState>` | 104 B/block |
| `Arc<PageMutationGate>` | 72 B/block；目录 stripe 锁共享 |
| `Arc<BlockWork>` | 48 B/block；ShardWork 共享 |
| 完整 `Arc<BlockId>` 及三个字符串 | 112 + L = 256 B/block |
| lifecycle ordered registry + pending set | 本例 49.38 B/block |
| `Box<PageAccess>` | 32 B/page |
| **一个 page/block 的增加量** | **561.38 B** |

这是刚完成生命周期绑定、尚未模拟 access checkpoint 的状态；dirty/retry/removed 集合
随后会随活动变化。TTI 关闭时，恢复不会为每个 block 创建这些 sidecar；在线
page 填充和相关删除路径仍可按需创建。关闭 TTI 的完整按需绑定实测为：

| 路径 | 每 block 增加 | 差异 |
| --- | ---: | --- |
| 已驻留的 paged block | **529.38 B** | 复用 resident PageTable，没有 PageAccess |

这些是 sidecar 尚未退休时的增加量，维护扫描满足退休条件后可回收。

源码：[恢复配置](../../crates/talon-worker/src/runtime/namespaces.rs)、
[LRU 恢复](../../crates/talon-worker/src/eviction/recovery.rs)、
[TTI 绑定](../../crates/talon-worker/src/runtime/page_maintenance.rs)。

## 容量、在线填充和热读附加项

哈希表不是每插入一项恰好分配一个键值。上面 16 B 键值的表有 32,768 个 buckets，
只存 16,384 项，所以约 **34 B/item**。一般应使用
`(bucket_count × (键值大小 + 1 控制字节) + 尾部控制区域) / live_items`。
Directory.slots 及其他容器的历史峰值容量也可能在删除后继续保留。

字面意义上，空容器的第一个 paged block 加一个 page，在同样 L=144 下需要
**1,268 B**；开启默认维护则为 **2,176 B**。这些数字仍排除了 scope 固定开销。
前面的 720.216 B 是大量 block 的均摊值。

容量队列恢复用排序后的批量 collect；16,384 项时是 **26.21 B/unit**。
同样数量的递增在线 insert 测得 **48.90 B/unit**。队列变动后的树形不同，不能一直套用恢复值。
在线首次写入还分别构造 catalog/runtime 身份，可能比恢复后的共享表示多
**64 + L = 208 B/block**；page mutation sidecar 另按上一节计算。

| 附加状态 | 用户态内存算法与条件 |
| --- | --- |
| 热文件 FD cache | 哈希键值 40 B（PathBuf 24 + CachedFd 16）、路径缓冲区 capacity、每个 `Arc<OwnedFd>` 24 B；另加哈希空槽。文件描述符的内核状态未测量 |
| FD cache 容量 | page 默认总量 16,384，跨 namespace 共享；不是每个 resident item 必有一个 FD。各 shard 独立分摊容量 |
| `WorkerRuntime.version_cache` | 每个已解析 object：哈希键值 104 B + bucket/path/version 字符串容量；与 eviction 的 VersionIndex 不同。恢复时为空 |
| version cache 的失效 | 3 秒 TTL 控制是否可复用；过期读取本身不删除 map entry，不能把它当成只保留最近 3 秒对象的有界缓存 |
| L1 | 默认关闭；开启后另加实际缓存 payload 和 L1 索引，不能计入固定 L2 item 元数据 |
| 在途请求、checkpoint、恢复数组 | 随并发、快照和恢复阶段变化；不是每个 resident item 的固定常驻成本 |

例如 FD 路径 capacity=96、哈希装载率约 0.5 时，请求内存约为
`(40+1)/0.5 + 96 + 24 = 202 B/缓存FD`，另有每 shard 的固定及尾部控制开销。
version cache 在本例字符串长度、相同装载率下约为 `105/0.5 + 144 = 354 B/object`。
这两个数字是基于实测类型布局的公式示例，不是本次对真实 FD/version cache 的分配实测。

源码：[FD cache](../../crates/talon-worker/src/fd_cache.rs)、
[version cache](../../crates/talon-worker/src/runtime.rs)、
[L1](../../crates/talon-worker/src/memory_store.rs)。

## 复现与证据

运行 [store_memory_probe.py](../../scripts/store_memory_probe.py)，它临时注入仅测试可见的
布局输出和 [Rust 分配探针](../../scripts/store_memory_probe.rs)，运行后
恢复所有生产源文件。请在开发容器内运行，使用与业务构建相同的缓存和输出挂载。不要与其他修改同一源码或使用该工作目录的构建同时运行。

```sh
python3 scripts/store_memory_probe.py
```

默认输出位于 `target/memory-analysis/current/probe.log` 和 `totals.tsv`；
原始基线保存在 `target/memory-analysis/pr675/`。`LAYOUT` 为 size/alignment，
`FIELD` 为实际字段 offset，`HEAP` 为阶段存活分配差值，`ALLOCS` 为各请求大小的
存活分配数变化，`TOTAL` 排除了上述 scope 固定开销。计数只包含单个选定测试；
没有运行其余 Worker 测试，也没有执行生产 RSS 或线上压力测试。

验证：探针通过（1 passed，699 filtered out），容器构建退出码 0；
已核对恢复条目数、resident page 数与淘汰字节计数。临时源文件修改已恢复。
本次构建报告另保存在 `target/memory-analysis/pr675/build-report.json`。

## 第一轮优化实测

本地分支 `analysis/pr675-block-page-memory`，在上述 PR #675 基线上实施：

- `object_checks` 的键从独立 `ObjectId` 改为按 object 比较的共享 `ObjectKey`，
  哈希键值从 80 B 降到 32 B；本例均摊从 162.001 B/object 降到 66.001 B/object。
- PageEntry 的访问令牌只保存 page 所需字段，并把 resident 放入原本恒为零的
  generation 最低位。完整 generation、计费、pin、选择状态均保留；
  PageEntry 从 96 B 降到 80 B，PageState 仍为 48 B。
- resident 和 catalog 的 PageMap 在 2～4 页时使用有序 Vec；1 页保持内联，
  5 页起使用 BTreeMap，删除后缩回相应表示。迭代顺序及 journal JSON 格式保持不变。

未采用此前 64 B PageEntry 的估算方案：`set_access` 支持来自不同 index 的令牌，
其 policy PageState 与生命周期 PageState 可以不同。第一轮保留两者及各自的所有权，
也保留 block clock 指针；共享这些字段需要进一步调整调用契约。
未缩窄整数，也未替换 OnceLock。

同一探针、同一批量恢复场景下，不含字符串的存活堆请求字节：

| resident pages/block | 优化前 B/block | 优化后 B/block | 节省 B/block | 降幅 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 846.63 | 734.63 | 112 | 13.2% |
| 2 | 2176.82 | 1016.82 | 1160 | 53.3% |
| 4 | 2325.21 | 1373.21 | 952 | 40.9% |
| 16 | 5920.79 | 5280.79 | 640 | 10.8% |
| 64 | 18274.12 | 16402.12 | 1872 | 10.2% |
| 256 | 73669.23 | 66165.23 | 7504 | 10.2% |

单页时 block 侧为 676.414 → 580.414 B，page 侧为 170.213 → 154.213 B：
**96 + 16 = 112 B**。原样本还减少了 object_checks 的 112 B 字符串，
因此包含字符串的合计为 **1102.627 → 878.627 B**，节省 224 B。

两页时非字符串节省为 **96 + (1248 − 296) + (144 − 32) = 1160 B**，
分别对应 object 检查索引、resident 页容器、catalog 页节点；不重复加 PageEntry 差值。

96 B 是本例哈希容量下的每 object 收益。同一 object 的多个 blocks 共同分摊。
共享键最多保留一个代表 block 的身份元数据，直到该 object 最后一个 policy unit 删除；
不保留数据页。代表 block 先被淘汰时，其 Packed 和字符串会延后释放，实际净收益
相应减少。新增测试覆盖跨 offset/version 的共享计数、检查时间及最后一项删除后的释放。

验证及证据：

- `cargo fmt --all --check`、Worker 全 features 二进制构建通过。
- Worker `--lib --bins --benches --all-features` Clippy（`-D warnings`）及
  `--all-features --no-deps` rustdoc（`-D warnings`）通过。
- 全目标 Clippy 被既有集成测试中的 `env!("CARGO_BIN_EXE_talon-worker")` 编译错误
  阻塞；构建当前二进制并显式提供该变量后仍有相同错误，未修改这些集成测试。
- Worker 单元测试：665 passed、29 failed、8 ignored。29 项失败位于 io_uring 初始化
  或启动路径，日志为 `Operation not permitted` 或随后 `rings never bound`；不声称全量通过。
- 新增测试覆盖有序容器升降级、range、原地修改、JSON 兼容、旧代际失效，以及 access
  清除不影响 resident。现有跨 index 令牌和 page 生命周期测试通过。
- 分配探针：1 passed；临时注入的测试代码已恢复。没有测量线上 RSS 或吞吐变化。
- 基线：`target/memory-analysis/pr675/totals.tsv`。
- 复测：`target/memory-analysis/pr675-round1/probe.log`、`totals.tsv`、`comparison.tsv`。
- 构建/测试报告：同目录 `tests-report.json`、`probe-report.json`、
  `build-and-integration-lint-report.json`、`lint-and-doc-report.json`。

复测命令：

```sh
python3 scripts/store_memory_probe.py --output target/memory-analysis/pr675-round1
```

## PR #677 上层发布

本层分支为 `cache-memory/paged-items`，基于 PR #677 的
`d87770f7d84e949d856d604c572b3889b8cc565b`。该基线未修改上述 paged 元数据结构。
PR #677 默认启用的 mimalloc 只安装在 Worker 二进制中，本报告的 library 分配探针
仍使用 System；请求字节统计不能直接等同于生产 mimalloc RSS。

该基线重新验证结果：670 个 Worker library 测试通过、29 个 io_uring 环境失败、
8 个 ignored；21 个 Worker 二进制测试通过。格式、全 features 构建、
库/二进制/benchmark Clippy 和 rustdoc 均通过。上述 29 项仍为权限拒绝或 ring 启动失败。
27 组分配场景的请求字节与第一轮结果逐项相同，探针 1 passed、707 filtered out。
没有运行全 workspace CI、吞吐 benchmark；开发镜像未安装 typos、lychee。
验证日志位于 `target/memory-analysis/publish-pr677/`，复测数据位于
`target/memory-analysis/pr677-round1/`。这些原始日志为本地工件，不纳入提交。
