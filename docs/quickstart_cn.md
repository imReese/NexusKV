# 🚀 NexusKV 开箱即用与快速上手指南

NexusKV 是专为大语言模型 (LLM) 推理集群打造的 **模型状态智能决策与 KV Cache 高性能存储引擎**。本指南提供开箱即用的安装、部署与集成指导。

---

## 目录
1. [环境准备](#1-环境准备)
2. [一键全量构建](#2-一键全量构建)
3. [三大部署形态开箱指南](#3-三大部署形态开箱指南)
   - [形态一：Python Wheel 嵌入（最简开箱）](#形态一python-wheel-嵌入最简开箱)
   - [形态二：Sidecar 守护进程部署](#形态二sidecar-守护进程部署)
   - [形态三：Go 分布式控制面部署](#形态三go-分布式控制面部署)
4. [运行基准测试 (Benchmark)](#4-运行基准测试-benchmark)
5. [常见问题 (FAQ)](#5-常见问题-faq)

---

## 1. 环境准备

建议的环境配置如下：
- **Python**: 推荐使用 `pyenv` 搭配 **Python 3.12**（支持 Python 3.11+）；
- **Go**: **Go 1.25+**（用于分布式控制面 `nexuskv-controlplane`）；
- **Rust**: **Rust 1.90+**（用于原生前缀树引擎与 PyO3 绑定）；
- **操作系统**: macOS (Apple Silicon UMA) 或 Linux (x86_64 / AArch64)。

---

## 2. 一键全量构建

在项目根目录下直接运行 `make` 或 `./build.sh`：

```bash
# 1. 克隆代码仓库
git clone https://github.com/imReese/NexusKV.git
cd NexusKV

# 2. 一键全量构建（编译 Go 控制面与 Rust 动态库）
make build

# 3. （可选）打包生成标准的 Python .whl 轮子安装包
make wheel
```

构建成功后：
- 二进制产物存放于：`bin/nexuskv-controlplane`；
- Python 安装包存放于：`dist/nexuskv_planner_native-*.whl`。

---

## 3. 三大部署形态开箱指南

### 形态一：Python Wheel 嵌入（最简开箱）

适用于单机多卡、单节点 vLLM / SGLang 推理服务。零进程开销。

```bash
# 安装生成的 wheel 包
pip install dist/nexuskv_planner_native-*.whl
```

在 vLLM 脚本中一行代码挂载：

```python
import vllm
from nexuskv.connectors.vllm import NexusKVPagedAttentionConnector

# 1. 初始化推理引擎
engine = vllm.LLMEngine.from_engine_args(engine_args)

# 2. 挂载 NexusKV 智能决策层
engine = NexusKVPagedAttentionConnector.attach(engine)
```

---

### 形态二：Sidecar 守护进程部署

适用于物理 GPU 节点多 Worker 伴生隔离场景。

```bash
# 1. 启动节点级 Sidecar 守护进程
./bin/nexuskv-sidecar \
  --listen=unix:///tmp/nexuskv.sock \
  --hbm-pool-gb=64

# 2. vLLM 节点连接 Socket 启动
export NEXUSKV_SIDECAR_ENDPOINT="unix:///tmp/nexuskv.sock"
python3 -m vllm.entrypoints.openai.api_server --model llama-3-70b
```

---

### 形态三：Go 分布式控制面部署

适用于千卡 PD 分离集群与高可用多租户场景。

```bash
# 1. 启动 Go 分布式控制面
./bin/nexuskv-controlplane \
  --config=config/controlplane.yaml \
  --listen=:8080 \
  --metrics-listen=:9090

# 2. Prometheus 接入 Metrics 监控
curl http://localhost:9090/metrics
```

---

## 4. 运行基准测试 (Benchmark)

直接运行双维度与多尺寸 KV Tensor 阶梯基准测试看板：

```bash
python3 tools/run_benchmarks.py
```

控制台将打印硬件感知信息、P50/P90/P99 微秒级 Latency 以及 Payload Bandwidth (GB/s)。

---

## 5. 常见问题 (FAQ)

- **Q: 为什么我运行脚本提示 `ModuleNotFoundError: No module named 'nexuskv'`？**
  A: 请先运行 `make build` 或确保 `PYTHONPATH=python` 已被添加。

- **Q: 如何运行单元测试？**
  A: 直接执行 `make test` 即可自动运行 Go、Rust 与 Python 的全量单元测试。
