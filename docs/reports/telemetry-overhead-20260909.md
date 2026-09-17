# Telemetry 修复与局部性能测量（2026-09-09）

结论：**开启 telemetry 的开销不能视为可忽略**。完成本轮优化后，短小本地 RPC
在 1% root sampling 下，配对耗时中位增幅约 6%；全采样约 61–63%。关闭路径没有
新增分配，但仍观察到约 1% 的耗时差异。当前证据不支持默认开启，也不证明关闭路径零开销。

## 修复和性能改动

- 高频 body/page/response 计数使用固定原子存储，结束时每个属性仅写一次。
  其他 detail 属性按 key 覆盖并限额 23，为生产 48-attribute 限额下的终态、摘要及
  tracing 自带字段保留空间。溢出标记 partial。10 万次更新的回归测试测得零次新增分配。
- Gateway 保留 Body 的 framing metadata，识别 Content-Length 完成、HEAD 和
  无 body 响应；真实提前 drop 仍为 cancelled。移除原 unfold 包装中的额外 body box。
- 控制 RPC negative Ack 正确标错；v2 与 v1 一样校验 Control 消息类型。
- read ID 从逐字节 format/collect 改为栈上十六进制编码。
- 出站 buffer 根据当前 tracestate 预留空间，不再一律预留完整的 1,026-byte
  入站 envelope 上限。无 tracestate 的典型预留为 79 bytes；未知宿主 context
  保守预留完整 W3C tracestate 容量。未采样 RPC 的分配次数保持不变。

## 测量对象与方法

基线为 `81e09496cc13cf270d1f919a56d109f6888b105f` 的独立只读来源快照。
当前样本使用修复后的工作区，源码哈希见 [environment.json](telemetry-overhead-20260909/environment.json)。
三种独立优化构建为原始 HEAD、当前代码仅传播依赖（compile-off）以及含 recording/export。
使用 Cargo release、相同现有锁定依赖版本；编译最多 2 个任务。

运行于 Intel Xeon Gold 6338，进程限制在 CPU 62、63；每次只有一个 probe。
7 轮确定性随机交错模式顺序；每个案例预热 1,000 次。
RPC 使用真正的 `WorkerClient::fetch_range`、连接池和 TCP，响应体为 64 B / 4 KiB，
每轮各 5,000 次。服务端是单独线程上的固定响应 stub，可接受 v1/v2，未启动真实 Worker。
客户端外围增加一个与 SDK 请求相同的 Operation scope。各模式的服务端业务逻辑相同。

另测每请求两个子 scope、无网络的微基准，每轮 20,000 次。
计时阶段不统计 allocator；另用 1,000 次调用统计客户端线程的 allocations 与分配字节。
分配字节是累计分配量（含 realloc 请求容量），**不是 RSS、峰值或存活内存**。

recording 使用生产同样的 48-attribute 限额，但 exporter 是同步内存丢弃 exporter。
没有 OTLP 编码、网络、真实 Collector，也没有生产 batch processor 的全部行为。
因此这些数值不能代替生产端到端延迟或导出总 CPU 成本。
`propagate` 从显式未采样父 context 继承；`unsampled` 每次请求以 root ratio=0
生成上下文。开启模式使用确认支持 v2 的 stub，故包含传播和 wire 成本。

## 优化后 RPC 结果

耗时列是 7 轮各自平均耗时的中位数。增幅是与**同轮**原始 HEAD 配对后
计算的百分比中位数，因此不必等于两个耗时中位数之比。单位 µs/请求。

| 响应字节 | 构建/模式 | 耗时 µs | 配对增幅 | 分配次数/请求 | 累计分配 bytes/请求 |
| --- | --- | ---: | ---: | ---: | ---: |
| 64 | baseline/off | 18.55 | +0.00% | 6.000 | 203.0 |
| 64 | compile-off/off | 18.60 | +0.76% | 6.000 | 203.0 |
| 64 | recording/off | 18.79 | +1.44% | 6.000 | 203.0 |
| 64 | recording/one-percent | 19.81 | +6.38% | 6.484 | 378.5 |
| 64 | recording/propagate | 19.38 | +4.35% | 6.000 | 282.0 |
| 64 | recording/sampled | 30.33 | +63.19% | 50.000 | 9058.0 |
| 64 | recording/unsampled | 19.59 | +5.18% | 6.000 | 282.0 |
| 4096 | baseline/off | 19.46 | +0.00% | 6.000 | 4235.0 |
| 4096 | compile-off/off | 19.76 | +0.89% | 6.000 | 4235.0 |
| 4096 | recording/off | 19.75 | +1.31% | 6.000 | 4235.0 |
| 4096 | recording/one-percent | 20.77 | +6.10% | 6.264 | 4366.7 |
| 4096 | recording/propagate | 20.19 | +3.05% | 6.000 | 4314.0 |
| 4096 | recording/sampled | 31.30 | +60.74% | 50.000 | 13090.0 |
| 4096 | recording/unsampled | 20.41 | +4.94% | 6.000 | 4314.0 |

全采样的 scope 微基准约 9.11 µs/请求，64 次分配；关闭 recording 的同一包装
约 0.082 µs，含 recording 但 runtime off 约 0.098 µs，纯业务基线约 0.069 µs。
小的无网络基线接近时钟和测量框架成本，不应把它的巨大百分比投射到完整读取。

各轮存在调度/频率噪声和 outlier，例如部分 RPC 配对出现负向差异。
全部区间保留在 [summary.json](telemetry-overhead-20260909/summary.json)。
7 轮不足以建立严格的生产等价界限。1% 采样的 allocation pass 只有 1,000 请求，
实际抽样次数存在随机波动；不能把小数分配次数当作精确长期均值。

## 两项性能优化的对照

在属性和终态修复之后、read ID/metadata 优化之前也运行了相同矩阵，保留
[原始结果](telemetry-overhead-20260909/raw-before-optimizations.jsonl)。
全采样 RPC 的分配次数由 86 降为 50；scope 微基准由 118 降为 64。
无 tracestate 的未采样 RPC 仍为 6 次分配，额外 buffer 容量由 1,026 B 降为 79 B。

两批测量的原始 HEAD 本身也有约 8–9% 的耗时漂移，不能直接比较两批绝对耗时
并把全部改善归因于代码。应分别看各批配对基线；allocation 次数的改善不受该频率漂移影响。

## 剩余风险与验证边界

- 新增功能仍默认 off。1% 和 100% recording 的成本明显，未通过设计中的默认开启门槛。
- 关闭路径的少量指令、future 布局和分支成本仍存在，不能只凭零 allocation 声称没有回归。
- 未测真实 Worker Tokio/Monoio 热 L1/L2、sendfile/splice、缓存 miss/refill、same-page
  竞争、SDK 多 block 扇出、Gateway 大 body、C/Python/Java 桥接性能。
- sampled refill 仍含 span 生命周期和共享 budget 同步，singleflight 归因会增加锁操作；
  diagnostic 会增加细粒度 span。未为它们给出吞吐或 CPU 回归上界。
- sampled Reqwest 路径仍要在消费 body 时统计部分字节；其聚合/复制成本需要真实 origin
  流量测量。本 TCP probe 不覆盖 backend HTTP。
- 未运行固定 offered-load、饱和吞吐、足量 P99.9、RSS/syscall、真实导出故障或滚动升级。
  原始文件中的每轮 P99 仅为该轮 5,000 次闭环请求的样本分位，不能作为生产尾延迟验收。

## 重现与原始证据

```sh
python3 bench/telemetry/run.py --output /tmp/talon-telemetry-results
python3 bench/telemetry/summarize.py /tmp/talon-telemetry-results
```

脚本会建立独立 HEAD 快照和临时构建目录，不更改其他工作区。
需要本机 socket 权限。`--build-only` / `--run-only` 可分开构建和测量。
再次运行请使用新的 output 目录，以保留历史结果。

- [优化后全部 147 条测量](telemetry-overhead-20260909/raw.jsonl)
- [统计汇总及每轮配对范围](telemetry-overhead-20260909/summary.json)
- [环境和源码指纹](telemetry-overhead-20260909/environment.json)
- [测量程序](../../bench/telemetry/overhead.rs) / [运行脚本](../../bench/telemetry/run.py)
