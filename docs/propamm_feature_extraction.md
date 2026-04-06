# PropAMM 交易的特征提取与识别指南
—— 基于真实链上套利交易的案例剖析

## 1. 什么是 PropAMM 交易？

PropAMM (Proprietary Automated Market Maker，专有自动化做市商) 是指由量化机构、做市商或高频 MEV（最大可提取价值）搜索者部署的闭源、私有智能合约。与公共 DEX（如 Raydium, Orca）不同，PropAMM 合约仅允许其所有者调用，主要用于在极短时间内跨多个流动性池执行复杂的套利、清算或夹子（Sandwich）攻击，以获取无风险利润。

本文档将以一笔真实的 Solana 链上高频套利交易作为缩影（Epitome），详细解析 PropAMM 交易的核心特征以及我们的系统是如何从原始区块数据中提取和识别这些特征的。

## 2. 案例剖析 (Case Study)

**交易签名 (Signature)**:
`uQcSGihfUkDKMWRAohHz7ueRuFcZDTBDGcRo9Yhei8m3VioBoYnihYKF2omtHewEJnSLbNkraGWGnSkUNT51Wju`

**所属区块 (Slot)**: `407001099`

**系统解析出的 JSON 摘要**:
```json
{
  "signature": "uQcSGihfUkDKMWRAohHz7ueRuFcZDTBDGcRo9Yhei8m3VioBoYnihYKF2omtHewEJnSLbNkraGWGnSkUNT51Wju",
  "block_time": 1773745630,
  "slot": 407001099,
  "arbitrage": [
    "So11111111111111111111111111111111111111112",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "So11111111111111111111111111111111111111112",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "E1kvzJNxShvvWTrudokpzuc789vRiDXfXG3duCuY6ooE"
  ],
  "profit": {
    "mint": "So11111111111111111111111111111111111111112",
    "amount": 0.01774694699997781
  },
  "propamm_programs": [
    "9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp"
  ],
  "via_aggregator": true,
  "jupiter_heavy": true,
  "trade_size": {
    "mint": "E1kvzJNxShvvWTrudokpzuc789vRiDXfXG3duCuY6ooE",
    "ui_amount_abs": 271.9272747160867
  }
}
```

通过这笔交易，我们可以总结出 PropAMM 的 5 大核心特征及系统提取方案。

## 3. 核心特征与提取逻辑

### 特征一：专有智能合约的调用 (Proprietary Contract Invocation)

* **表现形式**：交易的入口或核心逻辑由一个未公开开源的私有合约地址控制。
* **案例映射**：该交易触发了 `9H6tua7jkLhdm3w8BvgpTn5LZNU7g4ZynDmCiNN3q6Rp`，这是我们监控列表中的已知高频做市机器人合约。
* **提取逻辑**：
  1. 遍历交易的 `message.instructions` 和 `meta.innerInstructions`，提取所有被调用的 `programId`。
  2. 将这些 ID 与系统预设的 `programs.yaml` 进行求交集操作。如果有匹配项，即将其标记为潜在的 PropAMM 交易，并记录在 `propamm_programs` 数组中。

### 特征二：原子化套利闭环 (Atomic Arbitrage Loop)

* **表现形式**：资金在单个交易内经过多次转换，最终目的是实现低买高卖。由于是原子化交易，如果最终无利可图，整个交易会回滚 (Revert)。
* **案例映射**：`arbitrage` 字段清晰地展示了其代币转移路径：`SOL -> USDC -> SOL -> USDC -> WEN -> (返回自身)`。这说明机器人探知了多个资金池的价差并进行了多跳路由。
* **提取逻辑**：
  1. 系统通过解析 `meta.innerInstructions` 中由 Token Program 执行的 `transfer` 或 `transferChecked` 指令。
  2. 结合 `preTokenBalances` 和 `postTokenBalances` 映射出每个账户（Account）对应的代币（Mint）地址。
  3. 按指令执行的时间顺序，将涉及的 Mint 地址串联去重，形成套利流转路径 (Arbitrage Path)。

### 特征三：无风险净利润 (Risk-Free Profit Extraction)

* **表现形式**：交易发起者在不承担单边敞口风险的情况下，账户余额出现绝对的净增长。
* **案例映射**：该交易结束时，机器人的地址实现了 `0.0177 SOL` 的纯利润。
* **提取逻辑**：
  1. 系统识别交易的核心签名者 (Signer)。
  2. 对比该地址在 `preTokenBalances` 和 `postTokenBalances` 中每种代币的余额变化（原生 SOL 则对比 `preBalances` 与 `postBalances`）。
  3. 寻找网络手续费扣除后，净变化量大于 0 的代币。该正向余额差即为本次套利萃取出的利润 (`profit`)。

### 特征四：深度寄生聚合器路由 (Leveraging Aggregator Routing)

* **表现形式**：现代 PropAMM 很少自己管理复杂的底层 DEX 状态，而是将路由工作“外包”给聚合器（如 Jupiter），以便瞬间获取全网最佳流动性和最深盘口。
* **案例映射**：交易被标记了 `via_aggregator: true` 和 `jupiter_heavy: true`。其内部实际隐式调用了 Jupiter V6 备用合约 (`JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4`)。
* **提取逻辑**：
  1. 将所有调用的 `programId` 与 `aggregators.yaml` 中的聚合器地址列表进行比对。
  2. 若命中，则 `via_aggregator` 设为 true。
  3. 通过统计 Jupiter 相关指令在总调用指令中的占比，若超过设定阈值，则标记 `jupiter_heavy: true`，这通常说明该机器人并没有自己构建算法路由，而是高度依赖 Jupiter 的闪兑 API。

### 特征五：高资金周转与微薄利润率 (High Volume vs. Micro Margin)

* **表现形式**：为了赚取微弱的价差（可能仅有千分之一甚至万分之一），套利交易通常会动用庞大的资金（闪电贷或自有存量资金）来放大绝对收益。
* **案例映射**：为了赚取 0.0177 SOL 的利润，交易中途涉及了高达 `271.9 WEN` 的最高转账规模 (`trade_size`)。
* **提取逻辑**：
  1. 遍历所有的 Token 流转记录，找出绝对转移金额最大的单笔操作。
  2. 将其代币 Mint 和数额记录为本次交易的基准资金规模 (`trade_size`)。这一指标对于评估巨鲸机器人的资金体量及网络 TVL 的真实周转率至关重要。

## 4. 结语

该真实案例完美诠释了 Solana 链上高频 PropAMM 的生存法则：**私有合约操控 + 聚合器流动性寄生 + 多跳原子套利 = 无风险提取微利**。

我们的数据采集流水线通过深度解析 Solana JSON-RPC 返回的底层树状结构 (Transaction Metadata & Inner Instructions)，能够精准地从数十万笔杂乱的区块流水中，进行上述特征提取、链路还原和利润计算，为后续的宏观代币经济学分析和 MEV 建模提供了坚实、干净的数据基础。