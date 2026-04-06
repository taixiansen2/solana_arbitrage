# Solana PropAMM Arbitrage Collector

![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=flat&logo=docker&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=flat&logo=mongodb&logoColor=white)
![Solana](https://img.shields.io/badge/Solana-14F195?style=flat&logo=solana&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=flat&logo=python&logoColor=ffdd54)

一个基于 Docker 和 MongoDB 构建的 Solana 链上专有自动化做市商（PropAMM）套利数据采集、解析与分析流水线工具。

---

## 📖 项目简介

本项目旨在通过调用 Solana JSON-RPC 接口，精准扫描、过滤并提取与 PropAMM 相关的交易。流水线支持从原始区块扫描（`getBlock`），到提取特定程序交易签名，再到拉取详细解析的交易体（`getTransaction`）并进行高度精简的摘要汇总。

为了满足高并发和海量数据的需求，系统以 **MongoDB** 作为数据仓储，且**不落盘原始 JSON**，大幅节省存储空间，并配备了基于多线程的 RPC 请求机制。最终输出详尽的 Markdown 报告与可视化图表。

## ✨ 核心特性

- 🕵️ **智能过滤扫描**：基于配置好的 PropAMM 列表（`programs.yaml`）智能提取命中交易。
- ⚡ **多线程高并发**：内置可配置的 `FETCH_THREADS` 多线程架构，结合指数退避重试，完美适配不同频次与限额的 RPC 节点。
- 📦 **轻量级存储**：全内存级交易树解析，仅将核心摘要（如交易方向、净余额变化、涉及代币等）写入 MongoDB。
- 📊 **自动化报告**：自动分析入库的交易数据，生成包含 UTC 活跃时段、聚合器依赖度、命中比例等维度的图表和 `conclusion.md`。

---

## 🛠️ 快速开始

### 前置依赖
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/install/)
- 一个可用的 Solana RPC 节点 URL（如 Helius、QuickNode 等）

### 一键运行完整流水线
使用项目根目录下的自动化脚本，即可**按区块 Slot 闭区间**跑完：从扫块、获取摘要、落库到生成分析报告的全流程。

```bash
# 首次运行请赋予执行权限
chmod +x run_slot_range_pipeline.sh

# 交互式运行（按提示输入起始和结束 slot）
./run_slot_range_pipeline.sh

# 带参数运行（例如清理历史断点，限速 15 RPS，扫描 407000000 到 407001000）
RPC_URL="https://your-rpc-url" RATE_LIMIT_RPS=15 FETCH_THREADS=1 \
./run_slot_range_pipeline.sh --clear 407000000 407001000
```

### 常用选项与环境变量

| 选项 / 环境变量 | 默认值 | 描述 |
| :--- | :--- | :--- |
| `--clear` | - | 删除 `signatures.db` 与 slot 断点，重新开始扫块（新任务必备）。 |
| `--resume` | - | 从断点 `slot_range_checkpoint.txt` 续扫（**勿与 `--clear` 同用**）。 |
| `RPC_URL` | `公共节点` | 你的 Solana RPC 节点地址（建议使用专有高频节点）。 |
| `RATE_LIMIT_RPS` | `40` | RPC 请求限速（每秒请求数）。 |
| `FETCH_THREADS` | `1` | 请求交易摘要时使用的并发线程数。 |
| `FETCH_LIMIT` | `0` | 仅拉取前 N 条摘要至 Mongo，用于调试。`0` 表示不限制。 |
| `PROGRAMS_CONFIG` | - | 覆盖目标 program 列表配置文件路径。 |

---

## ⚙️ 架构与流水线解析

整个流程在 `docker-compose.yml` 编排下执行，核心组件分为 `collector` (Python 采集器) 和 `mongo` (数据库)。

流水线执行顺序如下：
1. **`discover` (可选)**：网页发现动态提取新出现的 PropAMM 地址。
2. **`collect_slot_range`**：利用 `getBlock` 批量获取目标区间内的所有区块，筛选包含 PropAMM 的交易签名并记录入 SQLite（`data/state/signatures.db`）。
3. **`fetch_transactions`**：使用多线程读取待处理的签名，调用 `getTransaction` 并在内存中解析逻辑，清洗后的数据**直接 Upsert** 到 MongoDB 的 `tx_summaries` 集合。
4. **`analyze`**：读取 MongoDB 的摘要数据，进行数据透视与聚合，最终生成自动化分析报告与可视化图表。

---

## 📁 产物结构

运行结束后，产生的高价值交付物位于以下目录：

- 📉 **`reports/conclusion.md`**：高度提炼的数据分析结论（含生成的 `.png` 图表）。
- 📝 **`reports/summary.md`**：详细的原始数据分布和计数。
- 🗄️ **MongoDB (`tx_summaries`)**：业务字段与精简交易流转数据。具体字段定义见 [`docs/MongoDB_tx_summaries字段说明.md`](docs/MongoDB_tx_summaries字段说明.md)。
- 💾 **`data/state/signatures.db`**：持久化的签名与抓取进度数据库。

---

## 💡 性能与时间估算

主网区块产生速度约 **2.3～2.5 slot/秒**。根据测试：
- 以太坊等单块交易较少的链，扫块极快；而 Solana 包含极多高频机器人（如 `SoLF` 等）。
- 1000 个区块可能命高达 15 万笔目标交易。
- 建议根据你的 RPC 套餐（如 10 RPS，40 RPS）以及机器配置，合理设置 `RATE_LIMIT_RPS` 与 `FETCH_THREADS`。
- 对于大范围分析（如整月），强烈建议屏蔽极其高频但分析价值低的噪音程序，以节省 RPC 调用费用与耗时。

---

## 🔧 高级：导出 MongoDB 数据
若需将 MongoDB 中的数据导出给其它下游使用，可运行：
```bash
docker compose run --rm -v $(pwd)/export_mongo.py:/export_mongo.py -v $(pwd):/out collector python /export_mongo.py
```
这会在根目录下生成按万条切分的 `export_data_partX.json` 文件集。