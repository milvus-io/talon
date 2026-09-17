# OpenTelemetry 实现与验证记录

日期：2026-09-09。工作区：`wet-bobcat`。这份记录描述本地实现和已运行的验证，
不代表已部署或通过性能上线门槛。配置和 SDK 使用见
[OpenTelemetry how-to](../how-to/opentelemetry.md)，统计契约见
[设计文档](../explanation/opentelemetry-design.md)。

## 实现范围

| 边界 | 实现 |
| --- | --- |
| 公共核心 | 有界 W3C carrier、显式/继承/root parent、逐次 poll scope、parent-based sampling、请求预算、有限诊断、取消/timeout 终态 |
| 传输 | v1/v2 双解码；v2 request-only TLV；显式 peer allowlist；每次 RPC attempt 重新注入；raw response 和原有数据发送路径 |
| Rust/C/Python/Java | 保留旧接口；Rust/C 显式请求 options；C 提交时复制；Python GIL 释放前捕获；Java dependency-free carrier adapter |
| Provider | binary 显式初始化；C/Python scoped dispatcher；宿主控制 Rust provider；有界 OTLP HTTP 导出和关闭 |
| Worker | Tokio/Monoio 请求 scope、版本解析、whole/paged refill、run commit、共享 follower links、分层命中和本地摘要 |
| Backend/Gateway | Reqwest execute/body/stream、显式重试和超时、Gateway 入站 W3C 和响应 body 生命周期 |
| 诊断 I/O | blocking queue/execution、write/fsync/rename；保留文件写入、持久化和业务 guard 语义 |
| 运维 | Collector/Tempo 示例、Grafana datasource/导出健康面板、指标、启用/回滚说明 |

实现跨越协议、SDK、运行时和部署，超过单个小变更的审阅规模。建议按公共核心、
协议、原生 SDK、Worker/Backend、语言入口与运维五个边界分别审阅。
没有修改缓存算法、业务重试次数、持久化格式或部署默认开关。

## 已运行的检查

Rust 使用当前工具链 1.96.1；Java 使用无网络、源码只读挂载的现有 JDK 容器，
以 `javac --release 17 -Xlint:all` 编译。

| 检查 | 结果 |
| --- | --- |
| `cargo fmt --all --check` / `git diff --check` | 通过 |
| `cargo build --workspace --exclude talon-python --all-targets --all-features --locked` | 通过 |
| `cargo test -p talon-telemetry --no-default-features --locked` | 2 passed |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | 通过 |
| `cargo test --workspace --exclude talon-python --all-features --locked` | 1,279 passed，21 ignored |
| `cargo test -p talon-python --features telemetry --locked` | 3 passed |
| `cargo check --workspace --no-default-features --locked` | 通过 |
| Java conformance 和 parent override/nesting/capability 检查 | 16 passed，21 个共享 vectors |
| `docker compose -f deploy/observability/otel/compose.yaml config --quiet` | 通过；未启动 stack |

localhost 测试使用 `NO_PROXY=no_proxy=127.0.0.1,localhost`。
Python 按仓库 CI 单独测试：`extension-module` 是 wheel 的链接模式，不能用于
需要链接 libpython 的测试可执行程序。21 个 ignored 测试没有计入通过项。

新增的关键断言包括：

- 真实 Tokio Worker 上的 v2 Stat、冷读、热读、cache-only miss，同连接切回 v1，
  原始 payload 不变，cache-only miss 不调用 origin。
- 真实 Monoio/io_uring 上的 v2 冷读、sendfile 热读及同连接 v1。
- 采样 Worker 的真实 whole refill 数量、validated/committed bytes、热读零回填；
  follower 捕获的旧 flight context 不随 map entry 的替换而改变。
- 显式 parent 覆盖、root 隔离、并发 yield 后上下文恢复、未采样子树、预算截断、
  cancelled 和外层 timeout 的 attempt 终态。
- 实际 HTTP socket 的成功 EOF、截断 body、partial bytes、body drop、显式超时；
  请求摘要 GET 次数和消费字节与六次测试请求一致，属性不包含测试 URL 的秘密参数。
- 慢/失败 exporter 下生产者完成入队尝试、队列不超过 4,096、容量丢弃和失败计数、
  SDK shutdown 剩余 span 的丢弃计数与队列归零。
- C header/symbol smoke、options size/version 校验、调用者销毁字符串后 carrier 有效。
- off 和显式未采样 carrier 下，预热后的 100 次 scope/child-scope 操作分配次数为零。
  这是窄范围回归断言，不包含网络缓冲区、语言桥接、宿主 OTel context 或端到端读取。

## 依赖和 MSRV

OTel/SDK/OTLP 固定为 0.28.0，tracing-opentelemetry 为 0.29.0。对新增的
Linux 依赖闭包检查了 package metadata：声明的 MSRV 均不高于 1.75；
Tonic 0.12.3 未在 manifest 声明，随包 README 声明 1.71.1。
没有提高 workspace 的 Rust 1.80 声明。

本机只有 1.96.1，未运行完整 Rust 1.80 编译。原始 Cargo.lock 已含要求更高
MSRV 的依赖，例如 clap 4.6.4 / base64ct 1.8.3 要求 1.85；因此当前工作区的
整体 1.80 兼容性不能由本次检查证明，也没有在这次 tracing 变更中重锁无关依赖。

## 解释与验收边界

- Wire v1 和既有 SDK 调用入口保持兼容；直接使用 `talon-transport` 的 Rust
  调用方需注意源码变化：`FrameHeader` 增加 `version` 字段（旧 struct literal
  应填 `version: 1` 或使用 `FrameHeader::new`），`FrameError` 增加
  `InvalidEnvelope` 变体，穷尽匹配需相应更新。新增 C 符号需要对应版本的库。

- 导出关闭是有界、尽力而为的。SDK 0.28 shutdown 只刷新一个批次，剩余数据
  计入 dropped；不能承诺进程关闭时 trace 完整。
- 摘要是本地记录的工作量，预算省略时是已知下界。跨 Worker 总量需要按
  read/refill ID 去重汇总，不能直接把 SDK 和 Worker 摘要相加。
- shared dependency 的完整性保守标记；未采样 leader、重试代际、late span、
  旧 peer、export drops 都可能造成部分 trace。
- HTTP attempt 表示 Reqwest execute 边界，保留其原有内部 retry/redirect 行为；
  不等价于一个物理网络发送或 S3 账单中的一条请求。
- Gateway response bytes 表示向 HTTP body consumer 交付的字节；Worker 记录成功
  写出的 payload。写失败后的部分发送量可能未知，不能当成远端确认接收量。
- Java adapter 传播宿主 carrier，不自动安装 Java exporter；FUSE 无应用 parent
  时只能建立本地 trace。Collector/Grafana 资产尚未在完整部署中验收。
- 未运行设计中的交错 A/B 性能矩阵、same-page 高竞争压测、端到端 allocation/RSS/
  syscall/尾延迟验收、真实 Collector 故障部署或滚动升级/回滚实验。
  所有这些仍是启用 standard 的发布门槛；默认 off 不变。

## 2026-09-09 审查修复后的补充验证

修复了重复属性累积及终态丢失、Gateway 正常 HTTP framing 被误报 cancelled、
negative Ack 被记录为成功，以及 v2 控制请求缺少消息类型校验。
同时减少 read ID 编码分配和出站 envelope 的过量预留。

- 工作区测试（排除 talon-python，all-features、locked）：1,282 passed，21 ignored，0 failed。
- 最后追加的最大 tracestate 预留回归与 envelope suite：4 passed；Gateway 真实 HTTP
  framing 回归重跑：1 passed。该追加用例在上述工作区测试之后单独执行。
- Python telemetry 测试 3 passed；telemetry no-default-features 测试 2 passed。
- workspace all-targets/all-features Clippy（warnings denied）、no-default-features check、
  rustdoc（warnings denied）、格式和 diff 检查通过。
- 重新生成的 21 条协议 conformance vectors 与工作区文件一致。

已补充交错 A/B 的客户端局部测量，方法、原始数据和限制见
[telemetry 性能报告](telemetry-overhead-20260909.md)。1% 采样的短小本地 RPC
耗时增幅约 6%，全采样约 61–63%；这些结果不代表真实 Worker 或生产 OTLP 导出的
端到端开销，也没有完成上文列出的完整性能矩阵。默认 off 保持不变。
