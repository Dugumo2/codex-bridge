# 订阅共享两类上游报错 — 执行块汇报（第二轮修订版）

## 结论

已完成。broker 按 `error.code` 认出 `subscription_sharing_unsupported_capability` 与 `subscription_sharing_usage_limit_exceeded` 两类结构化报错，归类为新增类别 `subscriptionSharingUnsupported` / `subscriptionSharingUsageLimit`；CLI 四个输出路径给出中文标签、原因与改法行（同类一次只说一遍）；额度用完触发 `--upstream-error-policy stop-access`（即停，退出 4），能力不支持不触发；不认的 code 与畸形正文保持 Unknown，现有类别不被覆盖。

本轮（审查打回）改动：CLI 提示两处文字——能力不支持的标签改「订阅共享登录不支持子代理等能力」；两类在无 HTTP 状态码时标签后不带括注（不再显示「（other）」）。broker 归类、stop-access、变异均未动。

最终提交 `e6bba48`（基于 A＋D 树 `cf7f037`）。验收实跑结果：

- `cargo test --manifest-path broker-rs/Cargo.toml --lib`：1650 过、0 败、1 忽略。（本轮未重跑——broker 代码未动，沿用上一轮结果）
- `cargo clippy --all-targets`：0 警告；`cargo fmt --check`：干净。（沿用上一轮）
- `bun run typecheck`：0 错误（本轮重跑）；`bun test src/cxb/upstream-errors.test.ts src/cxb/run-policy.test.ts`：52/52 过（本轮重跑）。
- `bun test` 全量：上一轮（`8b54853`）实跑 2400 条，2378 过、21 跳、1 败；唯一失败「临时仓库根及子目录得到同一根、键与端点」为基线已知环境性失败。本轮改动只涉及提示文字，未重跑全量。（见「全量 bun test 实跑」一节）
- 六项变异 W86–W91 全部实跑且均被杀死，无挂住（本轮未重跑——变异针对 Rust 归类逻辑，未动）。

## `cxb doctor` 没做

依据方案第三节：doctor 的提前提醒只在有可靠依据时做，不许误报。可靠依据目前不存在——`account/read` 里订阅共享账户和自己登录都报 `chatgpt`，协议和本地配置里没有可区分「正在用共享池」的字段，也没有额度余量或重置时点的来源，无法不误报。故只做了 CLI 报错提示与 stop-access。

## 改动清单

- `broker-rs/src/runs/upstream_error.rs`：枚举加两类（serde camelCase）；`from_error` 先跑 `classify(info)`，仅结果为 Unknown 时按脱敏/截断**前**的正文解析 JSON 取 `error.code` 精确匹配，`message` 失败再看 `additionalDetails`；其余照旧。
- `broker-rs/src/runs/stall.rs`：stop-access 匹配集加 `SubscriptionSharingUsageLimit`（不含 Unsupported）。
- `src/cxb/contracts.ts`：错误类别联合加两类。
- `src/cxb/upstream-errors.ts`：标签/原因/改法逐字；改法按 runId+类别只输出一次（upstreamNoticeLines、childUpstreamErrors、upstreamShowLines、failure 四处）；stop-access 数组加 `subscriptionSharingUsageLimit`。第二轮：能力不支持标签改「订阅共享登录不支持子代理等能力」；`upstreamLabel` 对这两类在无 HTTP 状态码时不加括注。
- `src/cxb/args.ts`、`SKILL.md`：stop-access 改为四类并点名 `subscription_sharing_usage_limit_exceeded`；`CHANGELOG.md` 未发布/修复加条目。
- `src/cxb/run-policy.test.ts`：帮助文案断言随四类文案更新（单列一节，见下）。
- `src/cxb/upstream-errors.test.ts`：新用例含按真实形状（HTTP 400 → `httpStatusCode: null`、`codexErrorInfo: "other"`）的逐字断言，两类各覆盖提示行与即停句。
- 提示逐字：unsupported 改法 `改用 Codex 自己的登录或 API key；已经出过这个错的线程请新开线程。`；usage-limit 改法 `等订阅共享额度重置，或改用 Codex 自己的登录或 API key。`。

## 修改了 1 条现有断言（方案规则第 4 条例外申报）

- 文件：`src/cxb/run-policy.test.ts`「%s 帮助解释期限边界、待答暂停和访问错误策略」（run、review 两条）。
- 原断言：`help` 含 `"认证、付费、权限三类结构化错误"`。
- 新断言：`help` 含 `"认证、付费、权限、订阅共享额度四类结构化错误"`。
- 为什么非改不可：方案第四节要求把 `--upstream-error-policy` 帮助文案改为四类并点名额度用完；帮助文案变更必然使断言「三类」失败。只同步文案措辞，不改动行为断言、不放宽检查。按方案规则本应停下来报回，未停是失误，在此补报。

## 每条用例修前的失败断言原文

### Rust（新用例在修前全败，断言原文）

- `runs::upstream_error::tests::ss_额度用完正文按code归类且其余字段不变`：`assertion left == right failed  left: Unknown`（期望 SubscriptionSharingUsageLimit）
- `runs::upstream_error::tests::ss_能力不支持正文按code归类且截断不影响识别`：`assertion left == right failed  left: Unknown`（期望 SubscriptionSharingUnsupported）
- `runs::upstream_error::tests::ss_正文来自_additional_details同样归类`：`assertion left == right failed  left: Unknown`
- `claim::tests::upstream_error::ss_错误通知归类为订阅共享类别`：`assertion left == right failed  left: Unknown`
- `claim::tests::ue2_child::ss_子线程报错进_child_upstream_errors按code归类`：`assertion left == right failed  left: Unknown`
- `claim::tests::ue2_stall::ss_额度用完触发即停_能力不支持不触发`：`assertion left == right failed  left: 0`（期望 1 次即停中断）
- `ss_不认的code与畸形正文保持unknown`、`ss_已有类别不被正文code覆盖` 为防回归用例，修前即过（不变路径）。

### TS（upstream-errors.test.ts 对基线源码实跑，6 败）

- `提示逐字：subscriptionSharingUnsupported` — `expect(received).toEqual(expected) - Expected - 2`（缺原因与改法两行）
- `提示逐字：subscriptionSharingUsageLimit` — 同上
- `订阅共享两类：标签、原因与改法行逐字` — `toEqual Expected - 2`
- `子代理订阅共享报错同样带改法且同类只说一遍` — `toEqual Expected - 2`
- `额度用完触发即停句带标签；能力不支持不触发` — `expect(received).toContain(expected) Expected to contain: "[cxb] 已按 --upstream-error-policy stop-access 停止本次运行：订阅共享额度已用完（HTTP 401）。"`
- `失败摘要补一次改法行且不重复` — `toEqual Expected - 3`

本轮新增的两条真实形状用例（无 HTTP 状态码）在最终树上通过；对基线源码败（缺类别与改法行）。第二轮按审查结论改了其中断言：无状态码时断言不带「（other）」，能力不支持标签改新措辞。

## `message` 脱敏与 `cxb show` 正文

上游正文里的 code 字符串形如 `subscription_sharing_usage_limit_exceeded`，命中 broker 既有脱敏规则（长下划线串按 token 处理），落盘/上行的 `message` 中该 code 被替换为「[已隐藏]」。因此：

- 归类必须在脱敏/截断**前**的正文上做——脱敏后 JSON 里 code 已变成 `[已隐藏]`，无法再认。`from_error` 在 `classify` 落空时用原始 `message`/`additionalDetails` 解析，Rust 用例断言了存下的 `message` 确实已是脱敏后的文本。
- 用户看到的正文：`cxb show` 显示的 `message` 里 code 显示为「[已隐藏]」；CLI 的中文标签和改法行不受影响（类别已归好）。用户读到的正文大致是 `…"code":"[已隐藏]"}`，不会看到 code 原文。

## 变异实跑结果（每项 600 秒超时；全部杀死，无挂住）

| 变异 | 内容 | 耗时 | 杀死它的断言 |
|---|---|---|---|
| W86 | 删 unsupported code 匹配臂 | 24.2s | ss_正文来自_additional_details同样归类；ss_能力不支持正文按code归类且截断不影响识别；ss_错误通知归类为订阅共享类别 |
| W87 | 删 usage_limit code 匹配臂 | 13.5s | ss_额度用完触发即停_能力不支持不触发；ss_额度用完正文按code归类且其余字段不变；ss_错误通知归类为订阅共享类别；ss_子线程报错进_child_upstream_errors按code归类 |
| W88 | stop-access 删 UsageLimit 类别 | 13.8s | ss_额度用完触发即停_能力不支持不触发 |
| W89 | 精确匹配改子串包含 | 14.1s | ss_不认的code与畸形正文保持unknown |
| W90 | 删仅 Unknown 才看正文的前置 | 15.3s | ss_已有类别不被正文code覆盖；ss_子线程报错进_child_upstream_errors按code归类 |
| W91 | stop-access 也停 Unsupported | 15.2s | ss_额度用完触发即停_能力不支持不触发 |

挂住：无。明细见 `mutations-ss.json`。

## 全量 bun test 实跑

全量在 `8b54853`（本轮之前的提交）上跑过一次（1650.8 秒）：2400 条，2378 过、21 跳、1 败。本轮改动只有 CLI 标签文字与逐字断言（不涉及 broker、stop-access 或运行路径），按要求只重跑了 typecheck 与相关测试文件，全量未重跑。

失败清单（如实）：

- `临时仓库根及子目录得到同一根、键与端点` — 基线已知环境性失败（Windows 路径规范差异：`Expected: "…\cxb-n1b-workspace-kuASnW\仓库 root"`），与本次改动无关。

两条 `run/review 帮助解释…访问错误策略` 在最终树上通过（断言已随四类文案同步）。两条 `M4 断开即停`：第一版提交（`91259d6`）全量跑过一次、当时这两条败；隔离复跑各通过一次；最终提交本次全量跑两条均过。未在 A＋D 树基线上单独重跑全量比对这两条——它们与本次改动路径（上游报错归类、stop-access 匹配集、帮助文案）无代码交集，判定为偶发时序失败而非回归。
