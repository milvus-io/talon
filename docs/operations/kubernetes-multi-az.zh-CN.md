# Talon Kubernetes 多 AZ 部署交付手册

本手册面向 Kubernetes 部署团队，适用于包含本次 multi-AZ 改动的 Talon 镜像和 SDK。
目标是 Coordinator/Worker 跨 AZ 部署，业务读请求留在本 AZ，且故障时不跨 AZ 重试。
本文配置、源码单测和 Helm 渲染已经纳入验证；实际集群的调度、网络、存储和 AZ 故障演练仍须在交付环境执行。

## 1. 部署契约

推荐三个 AZ：每 AZ 一个 Coordinator、至少两个 Worker；业务使用同 Pod 的 Gateway
sidecar，或直接使用配置了 zone 的 SDK。Coordinator 之间共享 Kubernetes 或 etcd
状态后端。Kubernetes API / etcd 自身也必须具备跨 AZ 高可用能力。

| 配置 | 交付值 | 作用 |
|---|---|---|
| `TALON_ZONE_AFFINITY` | `true` | 仅在本 AZ 的持久成员集合内计算 primary placement |
| `TALON_ZONE` | 实际 Pod 所在 AZ，或由 Worker/Gateway/SDK 初始化时查询 Node | 必须与 Node 标签及成员注册表完全一致 |

亲和性开关的程序默认值是 `false`。因此**只部署跨 AZ Pod 并不会自动开启亲和性**；
每个读请求发起端必须显式开启 `TALON_ZONE_AFFINITY=true`。
affinity 关闭时使用全局 placement；开启后没有跨 AZ 重试选项。

LOAD/BatchLoad 使用本 AZ primary，不会改派到远端 owner。本地 AZ 故障时，严格模式会返回错误，其他 AZ 存活不代表请求可以成功。
同 AZ 的其他候选是否被尝试仍由客户端的候选数量及错误分类决定；多部署 Worker
不等于每个块都有多副本。缓存通过各 AZ 独立回源预热，没有跨 AZ 自动复制保证。
各 AZ 应分别预留工作集容量和回源带宽。

## 2. 部署前检查

1. 使用支持当前 schema 6 持久成员协议的 Coordinator、Worker 和全部 SDK。
   不支持与历史 lease-only/schema-5 discovery 混用。所有镜像使用经过测试的固定 tag。
2. Helm 的 `multiAz` 模式要求 Kubernetes 1.30+；需要至少两个不同 AZ，并在每个
   AZ 准备有正确 `topology.kubernetes.io/zone` 标签、可调度且容量足够的节点。
3. 使用持久 PVC。每个 Worker 保存自己的 `worker_identity` 和缓存目录；不得共享
   同一目录启动多个 Worker。预先核对 StorageClass、PV 的 node affinity 和实际磁盘 AZ。
4. 准备后端凭据 Secret、管理 API token Secret、网络连通性及既有生产安全配置。
   Helm 不会创建对象存储账户、跨 AZ etcd 或业务 Gateway 的认证身份。
5. 已有集群必须先执行第 6 节迁移，不能直接通过开启 `multiAz` 给旧 Worker 改 zone。

```sh
kubectl get nodes -L topology.kubernetes.io/zone
kubectl get storageclass
kubectl -n talon get pvc
```

本地盘可以采用 [local PV 示例](../../deploy/kubernetes/worker-local-pv.example.yaml)：
`Retain` 回收策略、`WaitForFirstConsumer` 绑定和 PV node affinity。
每个 slot 准备独立 PV，容量至少覆盖 Worker 配置容量。若使用云盘，确认卷的 AZ
约束及首次绑定策略。不要把一个已有 AZ 的 PVC 挂载到另一个 AZ 来规避调度失败。

## 3. 新集群通过 Helm 部署

复制 [values-multi-az.example.yaml](../../deploy/helm/talon/values-multi-az.example.yaml)，
保存为 `values-production.yaml`，替换 zone 名、镜像 tag、StorageClass 和 Secret 名称：

```yaml
image:
  tag: replace-with-tested-multi-az-image
coordinator:
  backend: kubernetes
  replicas: 3
  auth:
    existingSecret: talon-admin-token
worker:
  replicas: 6
  persistence:
    enabled: true
    storageClass: talon-local
    size: 20Gi
  backend:
    existingSecret: talon-worker-backend
multiAz:
  enabled: true
  zones: [zone-a, zone-b, zone-c]
  clientServiceAccounts:
    - name: talon-gateway
      namespace: talon
```

```sh
helm lint deploy/helm/talon --strict --kube-version 1.30.0 -f values-production.yaml
helm template talon deploy/helm/talon --namespace talon \
  --kube-version 1.30.0 -f values-production.yaml > rendered-talon.yaml
kubectl apply --dry-run=server -f rendered-talon.yaml
helm upgrade --install talon deploy/helm/talon --namespace talon \
  --create-namespace -f values-production.yaml
kubectl -n talon rollout status deployment/talon-coordinator
kubectl -n talon rollout status statefulset/talon-worker
```

API dry-run 前应先创建 namespace 和必要的 CRD；Secret/PV 是否可用还须实际启动验证。
当前 chart 的 Worker 后端 Secret 必须包含 `azure-account` 和 `azure-sas` 键，见
[Worker Secret 示例](../../deploy/kubernetes/worker-secret.example.yaml)；S3/GCS 等其他
后端需要按对应 Worker 配置适配部署，不能只替换 Secret 名称。

外部 etcd 模式改为 `coordinator.backend=etcd`，并配置
`coordinator.etcd.existingSecret`，格式见
[etcd Secret 示例](../../deploy/kubernetes/etcd-secret.example.yaml)。两种后端均会创建
Worker zone 查询所需 RBAC；它不依赖 Coordinator 是否使用 Kubernetes 状态后端。

Chart 对 Coordinator 和 Worker 同时施加：指定 AZ 列表的 required node affinity、
`topology.kubernetes.io/zone` 上的 `maxSkew: 1`、`DoNotSchedule` 和
`minDomains: AZ 数量`。副本数不能少于 AZ 数量。缺失 AZ、卷绑定冲突或容量不足可能让
Pod 保持 Pending，这是需要处理的交付失败；不要通过移除硬约束来伪造跨 AZ 成功。
已有 Pod 的分布仍须现场核对，调度约束不会自动搬迁所有存量 Pod。

Worker 自动注入 `TALON_NODE_NAME=spec.nodeName`，通过仅含 `get nodes` 的 ClusterRole
查询 zone。`clientServiceAccounts` 只绑定已有业务 ServiceAccount，不创建它。
如果平台禁止 Node 查询，需用与硬调度约束一致的显式 `TALON_ZONE` 注入方案，不能
仅填一个全局固定 AZ。Chart 默认的跨 AZ discovery 路径需要上述 RBAC。

原始 YAML 入口见 [deploy/kubernetes](../../deploy/kubernetes/README.md)。它们是模板，
不包含 Helm 的完整 zone 清单及 `minDomains` 校验；部署团队须按同一契约补齐硬约束。
无论 etcd 还是 Kubernetes 后端，使用自动 zone 查询时都应配置
[node-zone-rbac.yaml](../../deploy/kubernetes/node-zone-rbac.yaml)。

## 4. 业务请求端配置

### Gateway sidecar：推荐的两跳本地方案

将 [S3 sidecar](../../deploy/kubernetes/gateway-s3-sidecar.yaml) 或
[Azure sidecar](../../deploy/kubernetes/gateway-azure-sidecar.yaml) 合入实际业务 Deployment。
替换业务/Gateway 镜像及凭据，保留 loopback bind；业务对象存储 endpoint 指向
`http://127.0.0.1:8080`（Azure 按模板附带 account 路径）。示例为 development mode，
适用于同 Pod loopback；不能直接创建面向网络的 Service 暴露该监听端口。

关键配置如下：

```yaml
spec:
  template:
    spec:
      serviceAccountName: talon-gateway
      containers:
        - name: talon-gateway
          # 其余镜像、协议、凭据、资源配置按对应模板设置
          env:
            - name: TALON_COORDINATOR_ADDR
              value: talon-coordinator.talon.svc.cluster.local:7000
            - name: TALON_ZONE_AFFINITY
              value: "true"
            - name: TALON_NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
```

这里是合并配置片段，不是可独立 apply 的完整 Deployment。业务 Deployment 也应按
第 3 节配置三 AZ spread、zone node affinity 和足够副本。业务在其他 namespace 时，
创建当地 ServiceAccount，并将该 namespace 填入 Helm `clientServiceAccounts`；
不要把 RBAC 绑定到错误 namespace。同 Pod 的第一跳为 loopback，第二跳由 Talon
placement 限制在本 AZ。

已有集中式 Gateway 必须保留其生产 TLS、认证与授权配置。Kubernetes Service 的
Topology Aware Routing / `trafficDistribution` 是偏好，可以跨 AZ；
`internalTrafficPolicy: Local` 限制的是本节点，也不是本 AZ。若第一跳要求严格同 AZ，
使用 sidecar 或每 AZ 独立 Service、仅选择该 AZ 的 Gateway，并确保业务调用匹配的
Service。不要仅凭 Service annotation 宣称两跳均不跨 AZ。

### Rust / C / Python / Java SDK

SDK 未配置 `TALON_ZONE` 时，采用与 Worker/Gateway 相同的 Node zone 查询机制。
业务 Deployment 的 Pod 模板加入以下配置，不需要为每个 AZ 硬编码 zone：

```yaml
spec:
  template:
    spec:
      serviceAccountName: talon-application
      automountServiceAccountToken: true
      containers:
        - name: application
          env:
            - name: TALON_NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: TALON_ZONE_AFFINITY
              value: "true"
```

创建业务 namespace 中的 `talon-application` ServiceAccount，再将它加入 Helm 配置：

```yaml
multiAz:
  clientServiceAccounts:
    - name: talon-application
      namespace: your-application-namespace
```

保留 values 中其他已配置的 ServiceAccount；上述列表是需要合并的片段。使用原始 YAML
时，将该账号与 namespace 加入 `node-zone-rbac.yaml` 的 ClusterRoleBinding subjects。
业务 Pod 仍须配置第 3 节的跨 AZ 调度约束。

SDK 使用 `KUBERNETES_SERVICE_HOST/PORT` 和挂载的 ServiceAccount token/CA，调用
`GET /api/v1/nodes/{TALON_NODE_NAME}`，读取 `topology.kubernetes.io/zone` 标签。
仅在客户端构造时查询一次，连接超时 2 秒，HTTP 请求超时 5 秒；读请求不会再查 API。
Rust/C/Python 复用现有 backend 解析器，Java 使用纯 JVM HTTPS 实现。
这个方案不依赖 Kubernetes 1.35；缺少凭据、403、超时或没有 zone 标签时保持 zone
未知，亲和性仍开启，读和 LOAD 失败关闭。修复初次发现失败后，需重新构造客户端。

Rust/Java 的显式 per-client `ZoneOptions` 会覆盖环境配置并跳过自动发现，包括显式
传入未指定的 zone（Rust `None` / Java `null`）；需审查业务代码是否覆盖了部署设置。C/Python 沿用 Rust 构造器，无 ABI 变更。

### 显式 zone 配置与 FUSE

设置 `TALON_ZONE` 会跳过 Node 查询，无需 Node 读取权限。FUSE 仍须显式配置 zone。
以下为 zone-a 业务 Deployment 合并片段；zone-b/zone-c 分别使用匹配的硬调度和 env 值：

```yaml
spec:
  template:
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: topology.kubernetes.io/zone
                    operator: In
                    values: [zone-a]
      containers:
        - name: application
          env:
            - name: TALON_ZONE
              value: zone-a
            - name: TALON_ZONE_AFFINITY
              value: "true"
```

显式注入时，必须验证 zone 与实际落地节点一致，且 Pod 重建后仍一致。
上述自动发现方案通过 Downward API 注入节点名，不要求平台将 Node zone 复制到 Pod 标签。

## 5. 验收：先证明成功请求本地，再验证故障关闭

```sh
kubectl -n talon get pods -o wide
kubectl get nodes -L topology.kubernetes.io/zone
kubectl auth can-i get nodes --as=system:serviceaccount:talon:talon-worker
kubectl auth can-i get nodes --as=system:serviceaccount:talon:talon-gateway
kubectl auth can-i get nodes --as=system:serviceaccount:your-application-namespace:talon-application
```

通过带认证的管理连接查看 `GET /api/v1/worker-membership` 和
`GET /api/v1/worker-discovery`，将 worker_id、zone、Serving 地址与 Pod/Node/PVC
逐一对应。不要用旧 `/api/v1/nodes` 代替持久成员清单。

| 场景 | 必须观察到的结果 |
|---|---|
| 三 AZ 正常 | 每 AZ 均有 Coordinator/Worker；各 AZ 客户端读成功且命中本 AZ Worker |
| 本 AZ 缓存冷 | 本地 Worker 回源并预热；重复读取命中本地缓存 |
| 本地 owner 离线或冲突 | 请求失败或按既定策略尝试同 AZ 候选；远端 Worker 不接到该读请求 |
| 本地 zone 无成员 / 客户端 zone 未知 | 严格模式读及 LOAD 失败，不选择全局 owner |
| 单 AZ 所有 Worker 不可达 | 该 AZ 请求失败；其他 AZ 独立验证仍能正常读 |
| LOAD / BatchLoad | 请求只发送至本 AZ primary；失败不改派远端 |
| Worker 保留 PVC 重启 | worker_id/zone 不变，地址或 instance_id 可变；恢复后重新本地服务 |
| Coordinator 单实例故障 | 其余实例服务，注册表不丢失；需单独测试客户端配置的控制面入口 |
| 业务 Pod 重调度 | sidecar/SDK 重新识别 zone；显式 zone 配置须与新节点匹配 |

测试用不同对象/范围产生实际网络读，并记录成功率、错误类型和目标地址。演练时应关闭
应用自行回源/跨 AZ 的上层重试，否则成功可能掩盖 Talon 本地失败。Gateway 默认
`TALON_GATEWAY_ROUTE=cache` 也可能自行回源兜底；验收可以用 SDK 或设为 `cache-only`
禁止 Gateway 自身绕过缓存路径。`cache-only` 不承诺 Worker 内部永远不回源。

Gateway 指标：`talon_gateway_zone_reads_total{zone_match}`、
`talon_gateway_zone_read_bytes_total{zone_match}`。开启亲和性后，成功的 Worker 读应为
`same`，`cross/unknown` 不增长。它们仅统计 Gateway 观测到的成功 Worker 读结果，不能证明业务到 Gateway 的第一跳、失败连接或 origin 流量没跨 AZ。
SDK 不暴露这些 Gateway 指标；应使用地址到 AZ 的映射、CNI/云网络流日志和请求计数
交叉验证。没有成功请求时，零跨 AZ 字节不是验收通过。

各语言的本地候选重试细节见 [策略参考](zone-affinity.md)。Talon 不提供跨 AZ
缓存副本或零流量费用保证。

## 6. 旧集群 zone 迁移

持久成员 zone 是逻辑身份的一部分。已有 `zone: null` 的 Worker 启用自动 zone 查询后，
若直接以相同 ID 注册不同 zone，会失败；心跳不会修复它。不要删除身份文件或 registry
来“解决”注册冲突。Chart 0.1 Deployment 到 StatefulSet 的卷迁移还须遵循
[滚动升级手册](../how-to/rolling-upgrade.md)，不能假设 Helm 自动保留旧临时卷。

推荐在维护窗口迁移，按以下顺序执行：

1. 备份完整成员注册表和存储清单，记录副本数、worker_id、PVC、PV、Node、实际 AZ。
   检查离线成员和 retirement tombstone；不得把这些记录当垃圾删除。
2. 暂停受影响业务请求，停止并隔离待迁移 Worker，等待实例撤销或租约过期。
   简单方案是将 Worker StatefulSet 缩至 0 并确认所有进程退出、PVC 保留；
   局部迁移必须能保证旧实例不会重新注册。记录恢复所需副本数，禁止两个实例共享磁盘。
3. 通过已认证的管理 API 再次 GET 完整成员注册表，在操作记录中保存返回文件。
   只修改经过核对的 member.zone；保持 worker_id、format_version、retired 和其他成员。
4. 使用返回的原始 `registry_revision`（包含合法的 null）执行完整 registry 的 CAS PUT。
5. HTTP 204 后确认 registry 中 zone 正确。若返回 409，重新 GET 并人工合并评审，
   不能替换 revision 后盲目重试。失败时保持旧实例隔离。
6. 配置与 PV 实际 AZ 匹配的 Worker zone discovery/调度，再恢复原副本数。
   验证相同 ID、相同缓存目录、正确 zone 和 Serving 状态，最后启用读端严格 affinity。

以下脚本只准备 CAS 请求；`zones.json` 是人工核对后的 `worker_id -> zone` 映射。
`TALON_ADMIN_URL` 指向受保护的管理 API，`TALON_ADMIN_TOKEN` 从部署团队的凭据渠道取得：

```sh
curl --fail --silent --show-error \
  -H "Authorization: Bearer ${TALON_ADMIN_TOKEN}" \
  "${TALON_ADMIN_URL}/api/v1/worker-membership" > membership-before.json
jq --slurpfile zones zones.json '
  {expected_registry_revision: .registry_revision,
   registry: (.registry | .members |= map(
     if $zones[0][.worker_id] != null
     then .zone = $zones[0][.worker_id] else . end))}
' membership-before.json > membership-update.json
```

检查映射中的每个 ID 都存在、没有漏改目标 ID，审阅完整 diff 并确认第 2 步隔离完成后：

```sh
curl --fail-with-body --silent --show-error -X PUT \
  -H "Authorization: Bearer ${TALON_ADMIN_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data-binary @membership-update.json \
  -o membership-update-response.txt -w '%{http_code}\n' \
  "${TALON_ADMIN_URL}/api/v1/worker-membership"
```

变更活跃成员的 zone 会改变 topology token 和 placement，缓存热度可能下降，应预留重新预热能力。
回滚使用支持同一持久成员协议的镜像，保留 PVC 和 ID。需要恢复旧 zone 时，仍须停止/
隔离实例并通过新的 revision 执行 CAS；不能直接提交旧备份覆盖新成员。
关闭 affinity 会恢复全局 placement，允许跨 AZ 流量，不属于保持严格 AZ 限制的回滚。

## 7. Kubernetes 参考

- [Topology spread 与 minDomains](https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/)
- [StorageClass volume binding mode](https://kubernetes.io/docs/concepts/storage/storage-classes/#volume-binding-mode)
- [Service traffic distribution](https://kubernetes.io/docs/concepts/services-networking/service/#traffic-distribution)
