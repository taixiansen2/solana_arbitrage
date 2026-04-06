# MongoDB 集合 `tx_summaries` 字段说明

`fetch_transactions` 在拉取 `getTransaction(jsonParsed)` 后**仅在内存**中解析，将精简结果写入 MongoDB。连接参数由环境变量控制（默认与 [`docker-compose.yml`](../docker-compose.yml) 一致）：

| 环境变量 | 含义 | 典型值 |
|----------|------|--------|
| `MONGODB_URI` | 连接串 | `mongodb://mongo:27017` |
| `MONGODB_DB` | 数据库名 | `propamm` |
| `MONGODB_COLLECTION` | 集合名 | `tx_summaries` |

首次拉取时会在 `signature` 上创建**唯一索引**（[`mongo_store.py`](../src/mongo_store.py)）。

---

## 文档字段（业务字段）

每条文档对应一笔**已通过 RPC 拉取正文**的交易；`_id` 为 MongoDB 自动生成。

| 字段 | 类型 | 含义 |
|------|------|------|
| `signature` | `string` | 交易签名（Base58），逻辑主键；与 SQLite `signatures` 中该行一致。 |
| `block_time` | `int` \| `null` | 链上 Unix 时间戳（秒，UTC），来自 RPC 结果 `blockTime`。 |
| `slot` | `int` \| `null` | 交易所在 slot，来自 RPC 结果 `slot`。 |
| `arbitrage` | `string[]` | **套利路径**：按时间顺序排列的代币转移链路（Token Mints），例如 `["USDC", "WSOL", "USDC"]`。 |
| `profit` | `object` \| `null` | **套利利润**：在整个代币流转闭环中，提取净余额增加最多的资产及数额。 |
| `profit.mint` | `string` | 利润对应代币 mint。 |
| `profit.amount` | `number` | 利润净增量（`float`）。 |
| `propamm_programs` | `string[]` | 命中的 **PropAMM 相关 program**：在 [`programs.yaml`](../data/config/programs.yaml) 中出现，且满足「指令树中出现 `programId`」或 `message.accountKeys` 公钥命中该列表（与 [`tx_summary.py`](../src/tx_summary.py) 中 `propamm_hits` 一致）。 |
| `via_aggregator` | `bool` | 是否在顶层/inner 指令中命中 [`aggregators.yaml`](../data/config/aggregators.yaml) **`programs`** 列表中的任一 program。 |
| `jupiter_heavy` | `bool` | **且** `via_aggregator == true` **且** Jupiter（`aggregators.yaml` 里 `labels` 值为 `jupiter_*` 的地址）在顶层 `instructions` + `meta.innerInstructions` 中出现次数 **≥ `JUPITER_HEAVY_MIN_IX`**（默认 `3`，见 compose）。 |
| `trade_size` | `object` | 规模近似：**单条** `(accountIndex, mint)` 上 **绝对 ui 变动最大**的一腿。 |
| `trade_size.mint` | `string` | 该腿 token mint。 |
| `trade_size.ui_amount_abs` | `number` | 该腿 ui 数量的绝对值（`float`）。 |

空对象 `{}`：无可用 token 余额前后对比算出一腿时可能出现（见 `_largest_token_ui_delta` 无结果）。

---

## 未入库的内容

下列内容**不会**写入 Mongo（见设计目标：不落盘完整 JSON）：

- 原始 `getTransaction` JSON、`logMessages` 全文、全部 `innerInstructions` 明细  
- `fee`、完整 `programs_involved` 列表、`err` 原文  
- 任意账户列表长串  

---

## 与流水线的关系

1. **`collect_signatures` / `collect_slot_range`**：只往 SQLite `signatures.db` 写 `fetched=0` 的签名队列。  
2. **`fetch_transactions`**：对 `fetched=0` 调 RPC；成功则 upsert 到本集合并标 `fetched=1`；`getTransaction` 为 `null` 时常标 `fetched=-1` 且**不写**本文档。  
3. **`analyze`**：读本集合生成 `reports/summary.md`、`reports/conclusion.md`（若 `reports/` 未忽略则会写入挂载目录）。

---

## 查询示例（`mongosh`）

```js
use propamm
db.tx_summaries.findOne()
db.tx_summaries.countDocuments({ via_aggregator: true })
db.tx_summaries.find({ jupiter_heavy: true }).limit(5)
```

---

*生成依据：[`src/tx_summary.py`](../src/tx_summary.py)、[`src/fetch_transactions.py`](../src/fetch_transactions.py)。*
