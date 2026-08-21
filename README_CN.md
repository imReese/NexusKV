<h1 align="center">NexusKV</h1>

<p align="center">
  <strong>让模型状态复用成为正确性决策，而不是一次“缓存命中”的猜测。</strong>
</p>

<p align="center">
  面向推理系统的引擎无关契约与决策层：识别、匹配、规划，<br/>
  并安全地物化可复用模型状态。
</p>

<p align="center">
  <a href="https://github.com/imReese/NexusKV/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/imReese/NexusKV/actions/workflows/ci.yml/badge.svg"></a>
  <a href="LICENSE"><img alt="Apache 2.0" src="https://img.shields.io/badge/license-Apache--2.0-blue.svg"></a>
  <img alt="API 状态：pre-1.0" src="https://img.shields.io/badge/API-pre--1.0-yellow.svg">
  <img alt="Go 1.25.9" src="https://img.shields.io/badge/Go-1.25.9-00ADD8.svg">
  <img alt="Python 3.11+" src="https://img.shields.io/badge/Python-3.11%2B-3776AB.svg">
</p>

<p align="center">
  <a href="README.md">English</a> ·
  <a href="#命中只是第一条事实">核心问题</a> ·
  <a href="#跟随一次状态复用决策">决策流程</a> ·
  <a href="#体验确定性核心">快速上手</a> ·
  <a href="#当前真正运行的部分">当前状态</a> ·
  <a href="#证据边界">验证</a> ·
  <a href="#按任务查文档">文档</a>
</p>

<p align="center">
  <img src="docs/assets/nexuskv-vision.svg" alt="NexusKV 愿景图：状态身份、匹配、兼容判断、复用规划、执行证据与未来数据面路径">
</p>

<p align="center">
  <sub><strong>产品愿景：</strong>实线边框表示仓库当前接口；虚线边框表示已设计但尚未完成验证的路径。</sub>
</p>

匹配 Token 并不难。真正困难的是判断对应的模型状态是否兼容、是否完整、
是否可达、是否值得复用，以及推理运行时能否安全消费。

NexusKV 为推理控制面与运行时适配器提供一套共享语言，用来回答：
**匹配到了什么状态、其中多少可以复用、哪条执行路径满足条件**。推理运行时
继续拥有目标内存分配与最终消费的决定权；可替换的数据面后端继续拥有物理
字节及其传输过程。

> [!IMPORTANT]
> NexusKV 处于 pre-1.0 阶段。当前仓库能够证明契约、匹配、有界本地存储、
> 确定性动作选择、降级与集成协议；尚不能证明原生加速器状态物化、真实
> RDMA 传输、Live Runtime 正确性或多节点生产就绪性。

## 命中只是第一条事实

真正节省模型计算之前，状态复用系统至少要回答以下问题：

| 问题 | NexusKV 中的表达 | 忽略后的风险 |
| --- | --- | --- |
| 这是同一个状态吗？ | 租户、命名空间、模型版本、状态语义、谱系、布局、DType、量化与并行域 | 跨模型或跨租户的静默错误 |
| 到哪里为止可以复用？ | 精确/最长前缀匹配、Matched Extent 与显式 Partial-hit Plan | 复用越过有效边界 |
| 目标端真的能消费吗？ | 能力、目标 Tier/Device/Buffer 约束与 Runtime Handoff | 元数据命中，但执行路径不可用 |
| 复用比重算划算吗？ | 成本输入、策略、拓扑与降级处置 | 搬运状态比重新计算更昂贵 |
| 字节真的到达了吗？ | Payload Handle、Transfer Session、完成证据与 Runtime 最终接收 | 把传输意图误报为传输完成 |

因此，NexusKV 将 **Match、Plan、Materialization 与 Consumption** 建模为
四个不同阶段。

## 跟随一次状态复用决策

~~~mermaid
flowchart LR
    query["有类型的状态查询"]
    match["精确或前缀匹配"]
    check{"兼容且满足<br/>准入条件？"}
    plan{"复用值得付出<br/>路径成本？"}
    action["物化 / 预取<br/>或路由到状态"]
    fallback["重算或跳过"]
    receipt["Payload Handle +<br/>Transfer Session"]
    runtime["Runtime 验证并<br/>消费状态"]

    query --> match --> check
    check -- 否 --> fallback
    check -- 是 --> plan
    plan -- 否 --> fallback
    plan -- 是 --> action --> receipt --> runtime
~~~

几个关键状态不会被混为一谈：

- MatchResult 证明发现了元数据，不证明 Payload 已经可用；
- PartialHitPlan 划分可复用与剩余工作，不证明复用一定更划算；
- Materialization Decision 记录执行意图，不代表传输已经完成；
- TransferSession 提供可观测的进度与结果元数据；
- 只有推理运行时才能接收状态到运行时拥有的内存并用于模型执行。

## 体验确定性核心

默认仓库门禁不需要模型下载、托管 API Key、加速器、推理运行时或远程
存储服务：

~~~bash
git clone https://github.com/imReese/NexusKV.git
cd NexusKV
make test
~~~

该命令运行 Go、Rust、Python 测试，以及 CPU-only 拓扑与读写精度 Fixture。
也可以分层执行：

~~~bash
GOTOOLCHAIN=go1.25.9 go test ./...
(cd rust && cargo test --workspace --locked)
PYTHONPATH=python python3 -m unittest discover -s python/tests -p "test_*.py"
python3 tools/generate_contracts.py --check
~~~

这些是确定性开发检查。除非某个测试明确连接真实后端，否则其中的硬件与
拓扑 Descriptor 都应视为 Fixture。

## 当前真正运行的部分

| 层 | 当前可运行实现 | 不包含在结论中的部分 |
| --- | --- | --- |
| 状态契约 | 版本化 JSON Schema，以及身份、Descriptor、Payload Handle、Transfer Session 的 Rust/Python 生成类型 | Schema 中存在标签，不代表状态类型或硬件路径已合格 |
| 索引与匹配 | Rust 精确/最长前缀匹配、显式部分命中规划和并发写时复制更新 | 收益判断、目标内存预留与字节搬运 |
| 本地存储 | 有界 Host DRAM Payload 存储、身份隔离与容量行为 | 分布式持久化或生产存储服务 |
| Planner 边界 | 通过薄 PyO3 Binding 暴露的 Rust 匹配器，以及确定性 Planner 输入输出 | 完整校准的生产成本模型 |
| Execution 边界 | 能力/策略感知的后端选择、结构化动作、降级、Payload Handle 与 Transfer Session | staged-copy 与 remote-store 仍是 Stub，不是真实物理传输 |
| Runtime 边缘 | 生命周期感知的 SGLang 与 vLLM Connector 接口 | 原生 Live Engine 状态导入和模型正确消费 |
| Control-plane 边缘 | 版本化策略交接、拓扑/控制 API 与单节点基础 | 多节点恢复与生产集群运行尚未合格 |
| Locus 集成 | 由 Rust Matcher 支撑的版本化 lookup/estimate/materialize HTTP Bridge | 跨进程证据仍是 zero-byte；物理状态传输未验证 |

## NexusKV 位于哪里

~~~text
推理控制面
  拥有请求准入与全局放置
            │
            ▼
运行时适配器
  翻译生命周期并构造有类型的 Descriptor
            │
            ▼
NexusKV
  身份 → 匹配 → 复用计划 → 执行意图 → 结果证据
            │                              │
            ▼                              ▼
数据面后端                           推理运行时
  保存并移动字节                       分配并消费状态
~~~

NexusKV 是位于中间的智能层：

- **推理控制面**决定整个推理请求在哪里运行；
- **运行时适配器**翻译引擎生命周期，并完成最终安全交接；
- **NexusKV**负责兼容性、发现、复用规划、动作选择、确定性降级与结果证据；
- **数据面**负责 Payload 容量、注册、传输、完成状态和后端故障；
- **推理运行时**负责设备内存、块/页表、Stream、Kernel 与最终消费。

运行时专属类型只停留在系统边缘。任何命中都不能绕过兼容性、授权、策略或
能力检查。

## 不局限于一种 KV Cache 形状

契约描述的是有类型的可复用模型产物，而不是硬编码某一种 Attention 布局：

| 契约维度 | 可能影响正确性的事实 |
| --- | --- |
| 语义身份 | 模型/Adapter 版本、Tokenizer/Template 身份、状态类型、租户/命名空间 |
| 逻辑范围 | Token、Page、Block、Layer、Checkpoint 谱系、可复用边界 |
| 物理布局 | Tensor Shape、DType、量化、Stride、并行切分 |
| 所在位置 | Device、Host DRAM、本地 SSD、Remote Shared Tier、拓扑 |
| 物化能力 | 支持的传输路径、目标能力、部分复用能力 |
| 结果证据 | 匹配分类、选定动作、降级原因、Payload Handle、传输结果 |

该抽象能够表达分页 Attention 状态和未来的有类型状态。**能表达不等于已实现**：
每一种状态仍需具体兼容规则、物化逻辑和验证证据，才能称为受支持。

## 当前集成接口

| 使用方或边界 | 当前接口 | 证据 |
| --- | --- | --- |
| Python Planning | Rust Matcher 的 PyO3 Binding | 真实语言边界和版本化 Planner 结果 |
| SGLang / vLLM | 生命周期感知的 Connector 接口 | 仅确定性生命周期与 Execution 一致性 |
| Locus 控制面 | lookup、estimate、materialize 的版本化 HTTP Bridge | 本仓库本地 HTTP，加上 Locus 配套跨进程编排 |
| 存储/传输后端 | 基线内存后端，以及 staged/remote Stub | 后端选择、降级和记录语义；无真实远程传输 |
| Go 控制面 | 版本化执行策略与拓扑基础 | 本地契约/控制行为；无生产多节点结论 |

## 证据边界

| 证据等级 | GitHub CI | 能证明什么 |
| --- | --- | --- |
| 静态与确定性 | 是 | Schema 一致性、匹配、动作顺序、策略、降级和本地存储 |
| 并发与本地 HTTP | 是 | 并发 Matcher，以及通过真实本地 Socket 的版本化 Bridge |
| CPU-only 拓扑 Fixture | 是 | Descriptor 数学与模拟控制流，不是物理多加速器执行 |
| 真实推理运行时 | 否 | 原生 Hook、Allocator 集成和模型正确的状态消费 |
| 物理数据移动 | 否 | DMA、RDMA、GPUDirect、远程网络与已验证字节数 |
| 生产集群 | 否 | 多节点恢复、租户隔离、持续负载、尾延迟与运维 |

任何性能结论都应附带可复现的 Workload、硬件、配置与方法。详见
[Benchmark 方法](docs/benchmarks/benchmark-methodology.md)；不要把模拟器吞吐或
Descriptor 数学解释为真实 Serving 性能。

## 按任务查文档

| 如果你希望…… | 阅读 |
| --- | --- |
| 理解职责与组件边界 | [架构](docs/design/nexuskv-architecture_cn.md) |
| 定义身份与兼容性 | [Attention 状态描述符](docs/design/attention-state-descriptor_cn.md) · [共享 Schema](docs/design/shared-schema_cn.md) |
| 修改匹配与部分命中 | [nxradixtree](docs/design/nxradixtree_cn.md) · [Python/Rust Planner Bridge](docs/design/python-rust-planner-bridge_cn.md) |
| 实现真实后端 | [执行边界](docs/design/execution-boundary_cn.md) · [Payload 传输契约](docs/design/payload-transfer-contract_cn.md) · [后端目录](docs/design/transport-backend-catalog_cn.md) |
| 连接推理控制面 | [Locus Bridge](docs/design/locus-bridge.md) |
| 区分实现状态与研究方向 | [白皮书：实现状态](docs/papers/beyond-kv-cache_cn.md#implementation-status-实现状态说明) · [Migration Status](docs/architecture/migration-status.md) · [路线图](docs/roadmap_cn.md) |

## 开发

提交变更前运行：

~~~bash
make fmt
make test
~~~

修改身份、兼容性、Payload 或传输语义时，应从版本化 Schema 开始，并保持
Rust/Python 生成类型一致。主 CI Matrix 还覆盖支持的 Go/Rust/Python 环境、
确定性 Benchmark 工具、拓扑 Fixture 与 Docker 构建。

## 范围

NexusKV 不是推理运行时、全局请求放置控制面、通用分布式数据库，也不是
自身即可完成物理传输的数据 Fabric。设计文档中出现某种状态、运行时、拓扑
或硬件路径，并不表示它已经实现。

## 许可证

NexusKV 采用 [Apache License 2.0](LICENSE) 许可证。
