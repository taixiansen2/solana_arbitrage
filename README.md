# PropAMM Solana 数据采集（Docker + MongoDB）

在容器内跑整条流水线：`discover`（可跳过）→ 合并配置 → **按 slot 扫块或按日拉签名** → `getTransaction` 摘要写入 **MongoDB**（不落盘原始 JSON）→ 生成 `reports/summary.md` 与 **`reports/conclusion.md`（含图表）**。

默认 RPC 与 Mongo 连接见 [`docker-compose.yml`](docker-compose.yml)。`docker compose run` 只会把 compose 里已声明的环境变量传入容器；**推荐用下方脚本**，避免漏传 `COLLECT_BY_SLOTS` 等参数。

## 时间估算（整月 slot 量级）

主网约 **2.3～2.5 slot/秒**，一个自然月（如 31 天）约合 **620 万～670 万 slot**。以下按 **约 700 万 slot** 粗算（仅理想值，未计入 429、重试、空块跳过）：

| 阶段 | 公式 | 40 RPS | 25 RPS | 10 RPS |
|------|------|--------|--------|--------|
| **扫块** `getBlock` | `slot 数 / RPS` | ≈ **49 小时** | ≈ **78 小时** | ≈ **8 天** |
| **拉摘要** `getTransaction` | `命中签名数 / RPS` | 视命中量而定（例如 80 万条约 **5.6 小时**@40 RPS） | 同上顺延 | 同上顺延 |

整条流水线时间 ≈ **扫块 ETA + 拉摘要 ETA** + 少量解析与写报告。先小范围试跑或设 `FETCH_LIMIT` 抽样再全量。

## 一键：输入区块范围跑完全流程

在项目根目录执行（需已安装 Docker / Docker Compose）：

```bash
chmod +x run_slot_range_pipeline.sh   # 首次
./run_slot_range_pipeline.sh --clear 403542958 403552958
```

不加参数时会**交互式询问** `SLOT_START` 与 `SLOT_END`（均为**闭区间**，含首尾）。

常用选项：

| 选项 / 环境变量 | 含义 |
|----------------|------|
| `--clear` | 删除 `signatures.db` 与 slot 断点，从头扫块 |
| `--resume` | 从 `data/state/slot_range_checkpoint.txt` 续扫（勿与 `--clear` 同时使用） |
| `--rps N` | RPC 限速（默认 **40**；写入 `RATE_LIMIT_RPS`） |
| `--no-build` | 跳过 `docker compose build` |
| `SKIP_DISCOVER=0` | 需要跑网页发现时（默认脚本为 `1` 跳过） |
| `FETCH_LIMIT=5000` | 只拉前 N 条摘要至 Mongo（调试；完整跑不设） |
| `PROGRAMS_CONFIG=...` | 覆盖 program 列表配置文件（相对 `arbitrage/` 的路径） |

脚本会自动设置 `COLLECT_BY_SLOTS=1`、`MAX_SLOT_RANGE` 为当前跨度（无需再设 `SLOT_RANGE_FORCE`）。mongo 随 `collector` 依赖启动。

## 产物（高层次）

- **MongoDB** 集合 `tx_summaries`：精简字段摘要  
- **`reports/conclusion.md`** + **`reports/figures/*.png`**：结论文档与图  
- **`data/state/signatures.db`**：签名与 `fetch` 进度  

首次或更换 RPC 时请核对 [`data/config/programs.yaml`](data/config/programs.yaml) 是否与目标 program 一致。
