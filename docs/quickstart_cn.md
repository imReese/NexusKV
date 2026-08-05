# 🚀 NexusKV 生产级快速上手指南

NexusKV 是专为大语言模型 (LLM) 推理集群打造的 **通用模型状态智能与分布式内存基础设施层**。本指南提供开箱即用的标准生产服务启动与部署指导。

---

## 1. 一键全量构建

在项目根目录下运行 `make` 构建：

```bash
# 1. 克隆代码仓库
git clone https://github.com/imReese/NexusKV.git
cd NexusKV

# 2. 一键构建 Go 分布式控制面与 Rust 原生数据引擎
make build

# 3. 打包并安装 Python 插件连接包
make wheel
pip install dist/nexuskv_planner_native-*.whl
```

---

## 2. 生产级推理服务启动指南 (Standard CLI Serving)

在生产集中式部署中，通常使用 `vllm serve` 或 `sglang serve` 标准命令行服务进行一键拉起。

### 方案 A：`vllm serve` 生产服务挂载

设置环境变量加载 NexusKV 插件后直接启动服务：

```bash
# 1. 自动挂载 NexusKV 智能状态层插件
export VLLM_PLUGINS=nexuskv

# 2. 生产环境标准拉起 vLLM API Server
vllm serve Qwen/Qwen2.5-72B-Instruct \
  --host 0.0.0.0 \
  --port 8000 \
  --kv-transfer-config '{"kv_connector": "NexusKVConnector", "kv_role": "kv_both"}'
```

---

### 方案 B：`sglang serve` 生产服务挂载

```bash
# 标准命令行拉起 SGLang 服务并挂载 NexusKV 传输通道
python3 -m sglang.launch_server \
  --model-path Qwen/Qwen2.5-72B-Instruct \
  --host 0.0.0.0 \
  --port 30000 \
  --kv-cache-connector nexuskv
```

---

### 方案 C：自定义 SDK / 嵌程式脚本挂载 (Programmatic Python SDK)

对于自研 Python 服务网关或定制化推理框架，可通过几行代码实现动态 Hook 挂载：

```python
import vllm
from nexuskv.connectors.vllm import NexusKVPagedAttentionConnector

# 初始化引擎
engine = vllm.LLMEngine.from_engine_args(engine_args)

# 挂载 NexusKV 智能状态与锁页内存控制器
engine = NexusKVPagedAttentionConnector.attach(engine)
```

---

## 3. 分布式控制面与 Sidecar 部署形态

### 形态一：Sidecar 节点伴生守护进程
适用于物理 GPU 节点与分布式 Worker 进程间隔离场景。

```bash
# 启动节点级 Sidecar 守护进程
./bin/nexuskv-sidecar \
  --listen=unix:///tmp/nexuskv.sock \
  --hbm-pool-gb=64
```

### 形态二：Go 分布式控制面 (Raft + Hash Ring)
适用于千卡 PD 分离集群与高可用多租户场景。

```bash
# 启动 Go 分布式控制面集群节点
./bin/nexuskv-controlplane --config=config/controlplane.yaml
```

---

## 4. 运行基准测试 (Benchmark)

```bash
# 运行端到端性能与吞吐量测试套件
make bench
```
