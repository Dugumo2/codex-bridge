//! 没有 raw 的 v2 跟进：从会话文件（rollout jsonl）补开始记录与消费记录。
//!
//! 上游只有 thread/start 能打开 raw，跨重启 `--resume` 的线程没有 raw，原来只能宽松配对。
//! Codex 把每个线程写进会话文件，路径在 thread/read 回包的 `thread.path`：
//! - 父线程文件里有协作工具调用（`function_call`，`call_id` 等于 Interacted 的条目 id），
//!   由此知道是 followup_task 还是 send_message，以及目标与正文摘要；
//! - 子线程文件里有收到的消息（`agent_message`），与 raw 的同形，所在的轮就是消费它的轮。
//!
//! 流程：Interacted 到达、父线程没有 raw、也没有开始记录时，不立刻定模式，进入「等文件」：
//! 这期间不配对，操作照常阻止收敛。在 [`ROLLOUT_WAIT_MS`] 内从父线程文件按 call_id 找到调用、
//! 且（followup_task 时）子线程文件已读通一次，就按严格模式走；找不到、读不了就和原来一样定为宽松。
//! 定案后按原样重放暂存的 Interacted 通知，此后一切沿用 raw 的规则（`v2_pairing`）：
//! send_message 走只入队，消费记录进 [`DeliveryFact`](super::v2_pairing::DeliveryFact) 一一配对。
//!
//! 状态机不碰文件：读取经 [`Action::ReadRollout`] 交给 core 的 IO 层，结果以
//! [`Event::RolloutRead`](crate::claim::Event::RolloutRead) 回来；路径用 thread/read 请求拿。
//! 只在有操作等文件或等文件里的消费记录时才轮询，操作有结果、运行结束、停止、连接或授权换代后立即停止。
//! 有 raw 的线程一次都不读。不存、不记任何正文，只用摘要与字段名。
use super::v2_pairing::{parse_call, parse_delivery, EvidenceSource, ParsedCall, RawCapability};
use super::*;
use crate::rollout::{
    RolloutChunk, RolloutCursor, RolloutError, RolloutReadRequest, RolloutRecord,
};
use serde_json::json;

/// 等父线程文件里的工具调用（以及子线程文件首次读通）的窗口。
pub const ROLLOUT_WAIT_MS: u64 = 2_000;
/// 有操作等文件时的轮询间隔。
pub const ROLLOUT_POLL_MS: u64 = 150;
/// 文件末尾窗口里的消息比等它的最早操作还早这么多，就不可能是它们的，不登记。
pub const ROLLOUT_FLOOR_SLACK_MS: u64 = 10_000;
/// 同时跟随的文件数上限。
pub const MAX_ROLLOUT_FILES: usize = 64;
/// 文件读取或路径请求失败后，这么久内不重试；过后遇到新跟进重新取路径、重新读。
pub const ROLLOUT_RETRY_MS: u64 = 30_000;
/// 逻辑到期并不释放物理 IO 占位，只有真实结果能收尾。
pub const ROLLOUT_READ_MS: u64 = 30_000;
pub const MAX_ROLLOUT_READS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadTicketState {
    Active,
    /// 已观察到断档，本步驱动时清理文件状态。
    Interrupted,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadTicket {
    pub id: u64,
    pub thread: String,
    pub conn_epoch: u64,
    pub deadline: u64,
    pub state: ReadTicketState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RolloutPathState {
    /// thread/read 在途（请求代号）。
    Requested(u64),
    Ready(String),
    /// 到 `next_at` 之前不重试，过后遇到新跟进重新取路径、重新读；原因进诊断。
    Failed(V2FallbackReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutFile {
    pub epoch: u64,
    pub path: RolloutPathState,
    /// None：下次读末尾窗口。停止跟随时清空，重新需要时从末尾重来。
    pub cursor: Option<RolloutCursor>,
    /// 在途读取的编号。
    pub reading: Option<u64>,
    /// 当前这段跟随里成功读过几次。
    pub reads: u32,
    pub next_at: u64,
    /// 这段从末尾窗口起读以来，被下限挡掉的消息里最新一条的行时间；
    /// 下限降到它之下时下一次派发从末尾窗口重读，补回被挡的消费记录。
    pub floor_dropped: Option<u64>,
}

/// 等文件的操作：暂存的 Interacted 通知，定案后按原样重放。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutWait {
    /// 已有等待直接接收对应调用，不依赖为未来通知预留的调用缓存有空位。
    pub call: Option<RolloutCall>,
    pub deadline_ms: u64,
    /// Interacted 到达时的墙钟毫秒，用来给末尾窗口里的旧消息定下限。
    pub wall_ms: u64,
    /// 建等待那一刻父线程带着有效的失记标志；等到期仍没拿到调用时按它判未证明，
    /// 不按到期那一刻再查标志（标志可能在等到期之前已失效）。
    pub lost_at_arrival: bool,
    pub child: String,
    /// Interacted 到达时接收方已观察到的轮次：退回宽松时据此还原旧版当时会配的那一轮。
    pub child_turns: Vec<String>,
    pub events: Vec<(Value, bool)>,
}

/// 父线程文件里读到的协作工具调用；只留身份、类型与摘要。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolloutCall {
    pub epoch: u64,
    pub at: u64,
    pub wall_ms: u64,
    pub trigger: bool,
    pub target: Option<String>,
    pub message: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RolloutState {
    /// 与配对文件生命周期分离；跨连接代际保留直到 IO 实际返回。
    pub rollout_reads: BTreeMap<u64, ReadTicket>,
    pub next_read_id: u64,
    pub files: BTreeMap<String, RolloutFile>,
    /// 被淘汰的冷却文件保留的失败截止：线程 →（连接代际, 冷却截止的单调毫秒, 原因）。上限同 `files`。
    pub evicted_cooldowns: BTreeMap<String, (u64, u64, V2FallbackReason)>,
    pub waits: BTreeMap<EvidenceKey, RolloutWait>,
    /// (父线程, call_id) → 调用。读到时对应的 Interacted 可能还没来。
    pub calls: BTreeMap<(String, String), RolloutCall>,
    /// 缓存写满放不下、又没有同键等待认领的调用：父线程 → (连接代际, 读到时刻 + EVIDENCE_TTL_MS)。
    /// 之后同父线程的等到期仍没拿到自己的调用时据它判未证明，不按宽松规则回退。
    pub lost_calls: BTreeMap<String, (u64, u64)>,
    /// 轮询计时：(到期单调毫秒, 代号)。
    pub timer: Option<(u64, u64)>,
    pub next_id: u64,
    /// 测试用：发出过的读取次数。
    pub reads_issued: u64,
}

enum Decision {
    Strict(RolloutCall),
    Fallback(V2FallbackReason),
    /// 调用明明读到过却被写满的缓存丢下：不是「缺调用」，操作置未证明。
    Unproven,
}

impl RolloutState {
    fn next(&mut self) -> u64 {
        self.next_id = self.next_id.saturating_add(1);
        self.next_id
    }
}

fn read_failure(error: RolloutError) -> V2FallbackReason {
    match error {
        RolloutError::Missing | RolloutError::Unreadable => V2FallbackReason::RolloutUnreadable,
        RolloutError::Shrunk => V2FallbackReason::RolloutLost,
    }
}

impl Machine {
    /// 先撤销，再退休；禁止换代、清文件或超时重新获得物理 IO 名额。
    fn retire_rollout_reads(&mut self, all: bool) {
        let retired: Vec<(u64, String)> = self
            .relations
            .aux
            .rollout
            .rollout_reads
            .values()
            .filter(|t| {
                t.state != ReadTicketState::Retired
                    && (t.state == ReadTicketState::Interrupted
                        || all
                        || t.deadline <= self.clock.mono_ms
                        || t.conn_epoch != self.relations.conn_epoch)
            })
            .map(|t| (t.id, t.thread.clone()))
            .collect();
        if retired.is_empty() {
            return;
        }
        let needed = self.rollout_needed();
        for (id, thread) in retired {
            if let Some(ticket) = self.relations.aux.rollout.rollout_reads.get_mut(&id) {
                ticket.state = ReadTicketState::Retired;
            }
            if let Some(file) = self.relations.aux.rollout.files.get_mut(&thread) {
                if file.reading == Some(id) {
                    file.reading = None;
                    file.cursor = None;
                    file.reads = 0;
                    if needed.contains(&thread) {
                        file.path = RolloutPathState::Failed(V2FallbackReason::RolloutLost);
                        file.next_at = self.clock.mono_ms.saturating_add(ROLLOUT_RETRY_MS);
                    }
                }
            }
        }
    }

    /// 退订或传输断档前派发的读取不能参与后续配对。
    pub(crate) fn invalidate_rollout_reads(&mut self, thread: Option<&str>) {
        for ticket in self.relations.aux.rollout.rollout_reads.values_mut() {
            if ticket.state == ReadTicketState::Active && thread.is_none_or(|t| t == ticket.thread)
            {
                ticket.state = ReadTicketState::Interrupted;
            }
        }
        // 断档一并退休在途的路径请求（None 表示全部线程）：线程仍被需要时
        // 本步驱动会按新代次重发，旧回包到达时因登记缺失或代次不符被拒。
        self.relations.aux.rollout.files.retain(|t, f| {
            !(thread.is_none_or(|x| x == t) && matches!(f.path, RolloutPathState::Requested(_)))
        });
    }

    /// 只有确认本连接里没送过 raw 的线程才读会话文件；送过 raw 的、或登记满了能力未知的，一次都不读。
    fn file_allowed(&self, thread: &str) -> bool {
        self.raw_capability(thread) == RawCapability::NotSeen
    }

    fn rollout_file_failed(&self, thread: &str) -> Option<V2FallbackReason> {
        let state = &self.relations.aux.rollout;
        let from_file = state
            .files
            .get(thread)
            .filter(|f| f.epoch == self.relations.conn_epoch)
            .and_then(|f| match f.path {
                RolloutPathState::Failed(reason) if self.clock.mono_ms < f.next_at => Some(reason),
                _ => None,
            });
        // 文件表里没有本代际未到期的失败时，查淘汰时保留的冷却。
        from_file.or_else(|| {
            state
                .evicted_cooldowns
                .get(thread)
                .filter(|(e, until, _)| {
                    *e == self.relations.conn_epoch && self.clock.mono_ms < *until
                })
                .map(|(_, _, reason)| *reason)
        })
    }

    /// Interacted 首次到达、父线程没送过 raw、也没有开始记录：能用会话文件时进入「等文件」。
    /// 返回 Err(原因) 表示文件已知不可用，按宽松回退并记下原因；Ok(false) 表示不适用。
    pub(super) fn rollout_try_wait(
        &mut self,
        key: &EvidenceKey,
        item: &Value,
        completed: bool,
    ) -> Result<bool, V2FallbackReason> {
        let Some(op) = self.relations.operations.get(key) else {
            return Ok(false);
        };
        // 发起方这一轮的归属还没定（等绑定）时同样等文件：调用证据与归属分开保存，
        // 定案时登记的开始记录归属留空，绑定后补上（`adopt_pending_starts`）。
        if !self.convergence_enabled()
            || op.kind != EvidenceKind::V2Interacted
            || (op.run_id.is_empty() && !self.awaiting_parent_binding(key))
            || op.outcome != EvidenceOutcome::Pending
        {
            return Ok(false);
        }
        let Some(child) = item
            .get("agentThreadId")
            .and_then(Value::as_str)
            .filter(|t| !t.is_empty() && *t != key.parent_thread_id)
        else {
            return Ok(false);
        };
        // 缓存已命中的留言先兑现身份：留言不纳管接收方，注定被拒的检查不适用于它。
        // 缓存未命中的平级、向上等操作仍直接走原来的拒绝。
        let cached = self.rollout_call(key).filter(|c| !c.trigger).cloned();
        if cached.is_none() && self.followup_rejection(key, child, &op.run_id).is_some() {
            return Ok(false);
        }
        if self.rollout_call(key).is_none() {
            if let Some(reason) = self.rollout_file_failed(&key.parent_thread_id) {
                return Err(reason);
            }
        }
        if self.relations.aux.rollout.waits.len() >= MAX_OPERATIONS {
            return Ok(false);
        }
        let child_turns = self
            .relations
            .aux
            .observed
            .get(child)
            .into_iter()
            .flatten()
            .map(|o| o.turn.clone())
            .collect();
        let wait = RolloutWait {
            call: cached,
            deadline_ms: self.clock.mono_ms.saturating_add(ROLLOUT_WAIT_MS),
            wall_ms: self.clock.now_ms,
            lost_at_arrival: self.rollout_lost_marked(&key.parent_thread_id),
            child: child.into(),
            child_turns,
            events: vec![(item.clone(), completed)],
        };
        self.relations.aux.rollout.waits.insert(key.clone(), wait);
        // 调用刚写进父线程文件：不等下一个轮询间隔，尽快读一次。
        let epoch = self.relations.conn_epoch;
        let now = self.clock.mono_ms;
        if let Some(file) = self
            .relations
            .aux
            .rollout
            .files
            .get_mut(&key.parent_thread_id)
            .filter(|f| f.epoch == epoch && matches!(f.path, RolloutPathState::Ready(_)))
        {
            file.next_at = file.next_at.min(now);
        }
        Ok(true)
    }

    /// 等文件期间同键的另一条 Interacted 通知只暂存，定案后一并重放。
    pub(super) fn rollout_hold(
        &mut self,
        key: &EvidenceKey,
        item: &Value,
        completed: bool,
    ) -> bool {
        let Some(wait) = self.relations.aux.rollout.waits.get_mut(key) else {
            return false;
        };
        if !wait.events.iter().any(|(_, c)| *c == completed) {
            wait.events.push((item.clone(), completed));
        }
        true
    }

    pub(crate) fn rollout_awaiting(&self, key: &EvidenceKey) -> bool {
        self.relations.aux.rollout.waits.contains_key(key)
    }

    /// 运行名下还在等文件的操作数；它们照常阻止收敛。
    pub(crate) fn rollout_awaiting_for_run(&self, run: &str) -> usize {
        self.relations
            .aux
            .rollout
            .waits
            .keys()
            .filter(|key| {
                self.relations
                    .operations
                    .get(*key)
                    .is_some_and(|op| op.run_id == run)
            })
            .count()
    }

    /// 已按文件证据定为严格、还在等消费记录的操作：它的接收方文件要继续跟随。
    fn rollout_strict_waiting(&self, op: &EvidenceOperation) -> bool {
        op.v2_source == V2EvidenceSource::RolloutFile
            && self.v2_awaits_pairing(op)
            && self.rollout_run_live(op)
    }

    fn rollout_run_live(&self, op: &EvidenceOperation) -> bool {
        self.runs
            .runs
            .get(&op.run_id)
            .is_some_and(|r| r.state == crate::runs::RunState::Running)
            && self
                .relations
                .contexts
                .get(&op.run_id)
                .is_some_and(|c| c.authority != Authority::Revoked)
    }

    fn rollout_wait_live(&self, op: &EvidenceOperation) -> bool {
        op.outcome == EvidenceOutcome::Pending
            && op.conn_epoch == self.relations.conn_epoch
            && if op.run_id.is_empty() {
                // 运行还不知道：只要还在等发起方这一轮的绑定就继续等。
                self.awaiting_parent_binding(&op.key)
            } else if matches!(
                self.relations.aux.parent_waits.get(&op.key),
                Some(ParentWait::Settle { .. })
            ) {
                // 共享服务器上这一轮还没定案是不是自己的，运行上下文这时还没建：这不是取消。
                // 定案或认领释放都会清掉这条等待，定案不是自己的操作转未证明，由上面的结果判断收尾。
                true
            } else {
                (!self.op_confirmed(&op.key) || self.operation_generation_current(op))
                    && self.rollout_run_live(op)
            }
    }

    /// 操作在等发起方这一轮的绑定（发起方是刚被跟进、新轮尚未绑定的子代理）。
    pub(super) fn awaiting_parent_binding(&self, key: &EvidenceKey) -> bool {
        matches!(
            self.relations.aux.parent_waits.get(key),
            Some(ParentWait::Binding { .. })
        )
    }

    pub(super) fn rollout_call(&self, key: &EvidenceKey) -> Option<&RolloutCall> {
        self.relations
            .aux
            .rollout
            .calls
            .get(&(key.parent_thread_id.clone(), key.item_id.clone()))
            .filter(|c| {
                c.epoch == self.relations.conn_epoch
                    && self.clock.mono_ms < c.at.saturating_add(EVIDENCE_TTL_MS)
            })
    }

    /// 父线程此刻带着本代际未失效的失记标志（写满的缓存丢下过它的调用）。
    fn rollout_lost_marked(&self, parent: &str) -> bool {
        self.relations
            .aux
            .rollout
            .lost_calls
            .get(parent)
            .is_some_and(|(epoch, until)| {
                *epoch == self.relations.conn_epoch && self.clock.mono_ms < *until
            })
    }

    fn rollout_decision(&self, key: &EvidenceKey, wait: &RolloutWait) -> Option<Decision> {
        let Some(op) = self.relations.operations.get(key) else {
            return Some(Decision::Fallback(V2FallbackReason::RolloutCancelled));
        };
        if !self.rollout_wait_live(op) {
            return Some(Decision::Fallback(V2FallbackReason::RolloutCancelled));
        }
        // raw 能力未知时禁止新派读；此前已从本代际文件读出的调用仍可区分留言与跟进。
        let call = wait.call.as_ref().or_else(|| self.rollout_call(key));
        // 只入队的留言不依赖接收方文件：按父调用类型定为严格，子文件失败或冷却不降级。
        if let Some(message) = call.filter(|c| !c.trigger) {
            return Some(Decision::Strict(message.clone()));
        }
        match self.raw_capability(&key.parent_thread_id) {
            RawCapability::NotSeen => {}
            RawCapability::Unknown if call.is_some() => {}
            RawCapability::Seen => return Some(Decision::Fallback(V2FallbackReason::StartMissing)),
            RawCapability::Unknown => {
                return Some(Decision::Fallback(V2FallbackReason::RawUnknown))
            }
        }
        let expired = self.clock.mono_ms >= wait.deadline_ms;
        if let Some(call) = call {
            // 父侧缺可比正文时先问子侧：能力未知或已知失败的子侧给出自己的原因。
            let raw = self.raw_capability(&wait.child);
            let unavailable = match raw {
                RawCapability::Unknown => Some(V2FallbackReason::RawUnknown),
                RawCapability::NotSeen => self.rollout_file_failed(&wait.child),
                RawCapability::Seen => None,
            };
            if let Some(reason) = unavailable {
                return Some(Decision::Fallback(reason));
            }
            // 没有可比的正文摘要：严格配对只会等到收敛期限，不如照原样宽松。
            if call.message.is_none() {
                return Some(Decision::Fallback(V2FallbackReason::RolloutCallMissing));
            }
            // 只有接收侧还能提供消费证据时，等待迟到的发起方路径才有意义。
            if self.v2_sender_path(op).is_none() {
                return expired
                    .then_some(Decision::Fallback(V2FallbackReason::RolloutSenderUnknown));
            }
            let ready = self
                .relations
                .aux
                .rollout
                .files
                .get(&wait.child)
                .is_some_and(|f| f.epoch == self.relations.conn_epoch && f.reads > 0);
            if raw == RawCapability::Seen || ready {
                return Some(Decision::Strict(call.clone()));
            }
            return expired.then_some(Decision::Fallback(V2FallbackReason::RolloutUnreadable));
        }
        if let Some(reason) = self.rollout_file_failed(&key.parent_thread_id) {
            return Some(Decision::Fallback(reason));
        }
        // 这个父线程读到过调用但被写满的缓存丢下：等到期仍没拿到自己的调用时
        // 不是「缺调用」，宽松回退会错绑接收方的轮次，置为未证明。
        // 标志按等待建立那一刻登记，也查此刻的实时判断：标志可能在等到期之前失效，
        // 也可能在建等待之后才出现（等待先建、父文件之后才因缓存写满丢下调用）。
        if expired && (wait.lost_at_arrival || self.rollout_lost_marked(&key.parent_thread_id)) {
            return Some(Decision::Unproven);
        }
        expired.then_some(Decision::Fallback(V2FallbackReason::RolloutCallMissing))
    }

    /// 定案：固定模式与来源，按原样重放暂存的 Interacted 通知；之后迟到的行不改写已定模式。
    fn rollout_decide(&mut self, key: &EvidenceKey, decision: Decision, actions: &mut Vec<Action>) {
        let Some(wait) = self.relations.aux.rollout.waits.remove(key) else {
            return;
        };
        let Some(op) = self.relations.operations.get(key).cloned() else {
            return;
        };
        let decision = match decision {
            // 父文件已确认的留言无需配对开始记录。
            Decision::Strict(call) if !call.trigger => Decision::Strict(call),
            Decision::Strict(call) => {
                let recorded = self.v2_record_start(
                    key.clone(),
                    op.run_id.clone(),
                    op.authority_gen,
                    ParsedCall {
                        call_id: key.item_id.clone(),
                        trigger: call.trigger,
                        target: call.target.clone(),
                        message: call.message,
                    },
                    EvidenceSource::RolloutFile,
                    call.wall_ms,
                );
                if recorded && self.v2_start(&op).is_some() {
                    Decision::Strict(call)
                } else {
                    Decision::Fallback(V2FallbackReason::StartMissing)
                }
            }
            other => other,
        };
        if let Some(op) = self.relations.operations.get_mut(key) {
            match &decision {
                Decision::Strict(call) => {
                    op.v2_strict = true;
                    op.v2_queue_only = !call.trigger;
                    op.v2_source = V2EvidenceSource::RolloutFile;
                    op.v2_fallback = None;
                    op.rollout_arrival = Some((wait.child.clone(), wait.child_turns.clone()));
                }
                Decision::Fallback(reason) => {
                    op.v2_fallback = Some(*reason);
                }
                Decision::Unproven => {
                    // 失记不是缺证据：仍记调用缺失的原因，结果置未证明，不宽松配对轮次。
                    op.v2_fallback = Some(V2FallbackReason::RolloutCallMissing);
                    op.outcome = EvidenceOutcome::Unproven;
                    // 与登记时已未证明的失记跟进同口径：按证据期限移出 pending，不等收敛总期限。
                    if !op.run_id.is_empty() {
                        self.convergences
                            .ledgers
                            .entry(op.run_id.clone())
                            .or_default()
                            .unproven_deadlines
                            .insert(key.clone(), op.expires_at_ms);
                    }
                }
            }
        }
        if matches!(decision, Decision::Fallback(_)) {
            self.rollout_fallback_baseline(key, &wait.child, &wait.child_turns);
        }
        for (item, completed) in &wait.events {
            self.item_evidence_replay(key, item, *completed, actions);
        }
        if !op.run_id.is_empty() {
            self.touch_context(&op.run_id, MemberChange::Activity, actions);
        }
    }

    /// 等文件期间接收方的轮次可能已经变了：退回宽松时只留旧版宽松当时会配的那一轮，其余并入基线。
    ///
    /// 旧版在 Interacted 到达时就按唯一候选配对：那时恰有一个未完成的候选就配它；一个都没有就配之后出现的
    /// 第一轮；那时就有多个候选的不猜（首轮身份不明）。等待期间新出现的轮次按观察先后排在后面。
    /// 到达时就有多个候选的不改动，仍按原规则判为身份不明。
    ///
    /// 观察缓存只留有限轮、先进先出：到达时的候选已被挤掉，或之后出现的第一轮可能已被挤掉时，
    /// 旧版当时的结论还原不了。不能把「现在看不到」当成「当时没有」，只接受此后真实开始的轮（`lenient_floor`），
    /// 否则等到收敛期限。到达时的轮还有一个在，后来的轮就一个也没被挤掉，缓存满了也能还原。
    fn rollout_fallback_baseline(&mut self, key: &EvidenceKey, child: &str, arrival: &[String]) {
        let Some(op) = self.relations.operations.get(key) else {
            return;
        };
        let baseline = op.baseline_turns.get(child);
        let is_new = |turn: &String| !baseline.is_some_and(|old| old.contains(turn));
        let observed: Vec<String> = self
            .relations
            .aux
            .observed
            .get(child)
            .into_iter()
            .flatten()
            .map(|o| o.turn.clone())
            .collect();
        let later_may_be_lost = if arrival.is_empty() {
            observed.len() >= MAX_OBSERVED_TURNS
        } else {
            !arrival.iter().any(|turn| observed.contains(turn))
        };
        let unrecoverable = later_may_be_lost
            || arrival
                .iter()
                .any(|turn| is_new(turn) && !observed.contains(turn));
        if unrecoverable {
            let floor = self.relations.aux.observation_seq;
            if let Some(op) = self.relations.operations.get_mut(key) {
                op.lenient_floor = Some(floor);
            }
            return;
        }
        let candidates: Vec<String> = observed.iter().filter(|t| is_new(t)).cloned().collect();
        let (before, after): (Vec<String>, Vec<String>) = candidates
            .into_iter()
            .partition(|turn| arrival.contains(turn));
        let keep = match before.len() {
            0 => after.first().cloned(),
            1 => before.first().cloned(),
            _ => return,
        };
        let excluded: BTreeSet<String> = before
            .into_iter()
            .chain(after)
            .filter(|turn| Some(turn) != keep.as_ref())
            .collect();
        if excluded.is_empty() {
            return;
        }
        if let Some(op) = self.relations.operations.get_mut(key) {
            op.baseline_turns
                .entry(child.to_owned())
                .or_default()
                .extend(excluded);
        }
    }

    /// 文件证据定为严格后接收方的文件读不了或定位不到当前轮：降为宽松并立即按宽松规则重试配对，
    /// 不让它卡到收敛期限。
    fn rollout_downgrade(
        &mut self,
        key: &EvidenceKey,
        reason: V2FallbackReason,
        actions: &mut Vec<Action>,
    ) {
        let Some(op) = self.relations.operations.get_mut(key) else {
            return;
        };
        op.v2_strict = false;
        op.v2_source = V2EvidenceSource::None;
        op.v2_fallback = Some(match reason {
            V2FallbackReason::RolloutTurnUnknown | V2FallbackReason::RawUnknown => reason,
            _ => V2FallbackReason::RolloutLost,
        });
        let arrival = op.rollout_arrival.take();
        let receivers: Vec<String> = op.receivers.iter().cloned().collect();
        let run = op.run_id.clone();
        if let Some((child, turns)) = arrival {
            self.rollout_fallback_baseline(key, &child, &turns);
        }
        for child in receivers {
            self.pair_first_turn(key, &child, actions);
        }
        if self.op_confirmed(key) {
            self.materialize(key, actions);
        }
        self.touch_context(&run, MemberChange::Activity, actions);
    }

    /// 每步末尾：定案到期或有结果的等待、补发路径与读取、维护轮询计时。没有文件模式的操作时什么都不做。
    pub(crate) fn drive_rollout(&mut self, actions: &mut Vec<Action>) {
        self.retire_rollout_reads(self.closing);
        if self.closing {
            self.rollout_stop(actions);
            return;
        }
        let state = &self.relations.aux.rollout;
        if state.waits.is_empty()
            && state.files.is_empty()
            && state.timer.is_none()
            && !state
                .rollout_reads
                .values()
                .any(|t| t.state != ReadTicketState::Retired)
        {
            return;
        }
        let epoch = self.relations.conn_epoch;
        let now = self.clock.mono_ms;
        self.relations
            .aux
            .rollout
            .calls
            .retain(|_, c| c.epoch == epoch && now < c.at.saturating_add(EVIDENCE_TTL_MS));
        self.relations
            .aux
            .rollout
            .lost_calls
            .retain(|_, (e, until)| *e == epoch && now < *until);
        let keys: Vec<EvidenceKey> = self.relations.aux.rollout.waits.keys().cloned().collect();
        for key in keys {
            let Some(wait) = self.relations.aux.rollout.waits.get(&key) else {
                continue;
            };
            if let Some(decision) = self.rollout_decision(&key, wait) {
                self.rollout_decide(&key, decision, actions);
            }
        }
        let lost: Vec<(EvidenceKey, V2FallbackReason)> = self
            .relations
            .operations
            .values()
            .filter(|op| self.rollout_strict_waiting(op))
            .filter_map(|op| {
                op.receivers
                    .iter()
                    .filter(|r| !op.rejected_receivers.contains_key(*r))
                    .find_map(|r| match self.raw_capability(r) {
                        RawCapability::Seen => None,
                        RawCapability::NotSeen => self.rollout_file_failed(r),
                        // 登记满了：接收方的文件不能再读，严格配对只会卡住。
                        RawCapability::Unknown => Some(V2FallbackReason::RawUnknown),
                    })
                    .map(|reason| (op.key.clone(), reason))
            })
            .collect();
        for (key, reason) in lost {
            self.rollout_downgrade(&key, reason, actions);
        }
        let needed = self.rollout_needed();
        // 同一编号只属于一段连续跟随，物理名额留到真实回包。
        let state = &mut self.relations.aux.rollout;
        let tickets = &mut state.rollout_reads;
        state.files.retain(|thread, file| {
            if file.epoch != epoch
                || (matches!(file.path, RolloutPathState::Failed(_)) && now >= file.next_at)
            {
                return false;
            }
            if needed.contains(thread) {
                return true;
            }
            if let Some(id) = file.reading.take() {
                if let Some(read) = tickets.get_mut(&id) {
                    read.state = ReadTicketState::Retired;
                }
            }
            file.cursor = None;
            file.reads = 0;
            if matches!(file.path, RolloutPathState::Ready(_)) {
                file.next_at = now;
            }
            !matches!(file.path, RolloutPathState::Requested(_))
        });
        let mut due: Option<u64> = self
            .relations
            .aux
            .rollout
            .waits
            .values()
            .map(|w| w.deadline_ms)
            .chain(
                self.relations
                    .aux
                    .rollout
                    .rollout_reads
                    .values()
                    .filter(|t| t.state != ReadTicketState::Retired)
                    .map(|t| t.deadline),
            )
            .min();
        for thread in &needed {
            let file = self.relations.aux.rollout.files.get(thread);
            match file.map(|f| &f.path) {
                None => {
                    if self.relations.aux.rollout.files.len() >= MAX_ROLLOUT_FILES {
                        // 量保留表容量前，先删掉表里已到期或换代作废的项。
                        self.relations
                            .aux
                            .rollout
                            .evicted_cooldowns
                            .retain(|_, (e, until, _)| *e == epoch && now < *until);
                        let files = &self.relations.aux.rollout.files;
                        let idle = |(t, f): &(&String, &RolloutFile)| {
                            !needed.contains(*t) && f.reading.is_none()
                        };
                        let cooling = |f: &RolloutFile| {
                            matches!(f.path, RolloutPathState::Failed(_)) && now < f.next_at
                        };
                        // 先选不在冷却中的空闲项；没有才选冷却中的，且保留表须有空位。
                        let evict = files
                            .iter()
                            .find(|e| idle(e) && !cooling(e.1))
                            .or_else(|| {
                                if self.relations.aux.rollout.evicted_cooldowns.len()
                                    >= MAX_ROLLOUT_FILES
                                {
                                    None
                                } else {
                                    files.iter().find(|e| idle(e) && cooling(e.1))
                                }
                            })
                            .map(|(t, _)| t.clone());
                        match evict {
                            Some(t) => {
                                // 淘汰冷却文件时保留它的失败截止：截止前原线程不提前重取路径。
                                if let Some(f) = self.relations.aux.rollout.files.get(&t) {
                                    if let RolloutPathState::Failed(reason) = f.path {
                                        self.relations
                                            .aux
                                            .rollout
                                            .evicted_cooldowns
                                            .insert(t.clone(), (epoch, f.next_at, reason));
                                    }
                                }
                                self.relations.aux.rollout.files.remove(&t);
                            }
                            None => continue,
                        }
                    }
                    let generation = self.relations.aux.rollout.next();
                    self.relations.aux.rollout.files.insert(
                        thread.clone(),
                        RolloutFile {
                            epoch,
                            path: RolloutPathState::Requested(generation),
                            cursor: None,
                            reading: None,
                            reads: 0,
                            next_at: now,
                            floor_dropped: None,
                        },
                    );
                    actions.push(Action::Request(RequestContext {
                        resume_retried: false,
                        stream_id: None,
                        review_id: None,
                        client: None,
                        token: None,
                        operation: Operation::RolloutPath {
                            thread: thread.clone(),
                            generation,
                        },
                        method: "thread/read".into(),
                        params: Some(json!({"threadId": thread, "includeTurns": false})),
                    }));
                }
                Some(RolloutPathState::Ready(path)) => {
                    let f = file.expect("文件登记存在");
                    if f.reading.is_some() {
                        continue;
                    }
                    if f.next_at > now {
                        due = Some(due.map_or(f.next_at, |d| d.min(f.next_at)));
                        continue;
                    }
                    if self.relations.aux.rollout.rollout_reads.len() >= MAX_ROLLOUT_READS {
                        continue;
                    }
                    let Some(id) = self.relations.aux.rollout.next_read_id.checked_add(1) else {
                        continue;
                    };
                    let floor = self.rollout_floor(thread);
                    let reread = f.floor_dropped.is_some_and(|at| at >= floor);
                    let fresh = reread || f.cursor.is_none();
                    let request = RolloutReadRequest {
                        id,
                        thread: thread.clone(),
                        conn_epoch: epoch,
                        path: path.clone(),
                        cursor: if reread { None } else { f.cursor.clone() },
                    };
                    let deadline = now.saturating_add(ROLLOUT_READ_MS);
                    let state = &mut self.relations.aux.rollout;
                    state.next_read_id = id;
                    state.rollout_reads.insert(
                        id,
                        ReadTicket {
                            id,
                            thread: thread.clone(),
                            conn_epoch: epoch,
                            deadline,
                            state: ReadTicketState::Active,
                        },
                    );
                    due = Some(due.map_or(deadline, |d| d.min(deadline)));
                    state.reads_issued = state.reads_issued.saturating_add(1);
                    let f = state.files.get_mut(thread).expect("文件登记存在");
                    if fresh {
                        f.floor_dropped = None;
                    }
                    f.reading = Some(id);
                    actions.push(Action::ReadRollout(request));
                }
                Some(RolloutPathState::Requested(_) | RolloutPathState::Failed(_)) => {}
            }
        }
        self.rollout_schedule(due, actions);
    }

    /// 需要跟随的文件：等文件的操作的父线程与接收方，以及按文件证据定为严格、还在等消费记录的接收方。
    /// 只读确认没有 raw 的线程：有 raw 的、登记满了能力未知的都不读。
    fn rollout_needed(&self) -> BTreeSet<String> {
        let mut needed = BTreeSet::new();
        for (key, wait) in &self.relations.aux.rollout.waits {
            if !self
                .relations
                .operations
                .get(key)
                .is_some_and(|op| self.rollout_wait_live(op))
            {
                continue;
            }
            needed.insert(key.parent_thread_id.clone());
            needed.insert(wait.child.clone());
        }
        for op in self.relations.operations.values() {
            if !self.rollout_strict_waiting(op) {
                continue;
            }
            for r in &op.receivers {
                if !op.rejected_receivers.contains_key(r) {
                    needed.insert(r.clone());
                }
            }
        }
        needed.retain(|t| self.file_allowed(t) && self.rollout_file_failed(t).is_none());
        needed
    }

    fn rollout_schedule(&mut self, due: Option<u64>, actions: &mut Vec<Action>) {
        let now = self.clock.mono_ms;
        let Some(at) = due else {
            if self.relations.aux.rollout.timer.take().is_some() {
                actions.push(Action::Unschedule {
                    key: crate::claim::TimerKey::RolloutPoll,
                });
            }
            return;
        };
        if self
            .relations
            .aux
            .rollout
            .timer
            .is_some_and(|(d, _)| d == at)
        {
            return;
        }
        let generation = self.relations.aux.rollout.next();
        self.relations.aux.rollout.timer = Some((at, generation));
        actions.push(Action::Schedule {
            token: Self::relation_token(),
            kind: TimerKind::RolloutPoll { generation },
            delay_ms: at.saturating_sub(now),
        });
    }

    fn rollout_stop(&mut self, actions: &mut Vec<Action>) {
        self.retire_rollout_reads(true);
        let state = &mut self.relations.aux.rollout;
        state.waits.clear();
        state.files.clear();
        state.evicted_cooldowns.clear();
        state.calls.clear();
        if state.timer.take().is_some() {
            actions.push(Action::Unschedule {
                key: crate::claim::TimerKey::RolloutPoll,
            });
        }
    }

    pub(crate) fn rollout_timer(&mut self, generation: u64) {
        if self
            .relations
            .aux
            .rollout
            .timer
            .is_some_and(|(_, g)| g == generation)
        {
            self.relations.aux.rollout.timer = None;
        }
    }

    /// thread/read 回包：只认协议返回的、且通过校验的那一个路径。
    pub(crate) fn rollout_path_reply(
        &mut self,
        thread: &str,
        generation: u64,
        result: &Result<Value, Value>,
    ) {
        let epoch = self.relations.conn_epoch;
        let Some(file) = self.relations.aux.rollout.files.get(thread) else {
            return;
        };
        if file.epoch != epoch || file.path != RolloutPathState::Requested(generation) {
            return;
        }
        if !self.rollout_needed().contains(thread) {
            self.relations.aux.rollout.files.remove(thread);
            return;
        }
        let path = result
            .as_ref()
            .ok()
            .and_then(|v| v.get("thread"))
            .filter(|t| t.get("id").and_then(Value::as_str) == Some(thread))
            .and_then(|t| t.get("path"))
            .and_then(Value::as_str)
            .filter(|p| crate::rollout::validate_path(p, thread));
        let now = self.clock.mono_ms;
        let file = self
            .relations
            .aux
            .rollout
            .files
            .get_mut(thread)
            .expect("路径请求仍有效");
        (file.path, file.next_at) = match path {
            Some(path) => (RolloutPathState::Ready(path.into()), now),
            None => (
                RolloutPathState::Failed(V2FallbackReason::RolloutPathUnavailable),
                now.saturating_add(ROLLOUT_RETRY_MS),
            ),
        };
    }

    /// 一次读取的结果：调用进缓存，消息按 raw 的规则登记消费记录；读不了就记失败原因。
    pub(crate) fn rollout_read_done(
        &mut self,
        id: u64,
        thread: &str,
        conn_epoch: u64,
        result: Result<RolloutChunk, RolloutError>,
        actions: &mut Vec<Action>,
    ) {
        let now = self.clock.mono_ms;
        let epoch = self.relations.conn_epoch;
        // 身份只认派读时冻结的票据，绝不以当前 file.reading 补造身份。
        let Some(ticket) = self.relations.aux.rollout.rollout_reads.get(&id) else {
            return;
        };
        if ticket.thread != thread || ticket.conn_epoch != conn_epoch {
            return;
        }
        self.retire_rollout_reads(false);
        let ticket = self
            .relations
            .aux
            .rollout
            .rollout_reads
            .remove(&id)
            .expect("已核验票据");
        if ticket.state == ReadTicketState::Retired {
            return;
        }
        let followed = self.rollout_needed().contains(thread);
        let Some(file) = self.relations.aux.rollout.files.get_mut(thread) else {
            return;
        };
        if file.reading != Some(id) || file.epoch != epoch {
            return;
        }
        file.reading = None;
        if !followed {
            file.cursor = None;
            file.reads = 0;
            file.next_at = now;
            return;
        }
        let chunk = match result {
            Ok(chunk) => chunk,
            Err(error) => {
                file.path = RolloutPathState::Failed(read_failure(error));
                file.next_at = now.saturating_add(ROLLOUT_RETRY_MS);
                file.cursor = None;
                return;
            }
        };
        file.cursor = Some(chunk.cursor.clone());
        file.reads = file.reads.saturating_add(1);
        file.next_at = if chunk.more {
            now
        } else {
            now.saturating_add(ROLLOUT_POLL_MS)
        };
        let raw = !self.file_allowed(thread);
        let floor = self.rollout_floor(thread);
        let mut turn_lost = false;
        for entry in chunk.entries {
            match entry.record {
                RolloutRecord::Call { item } => {
                    let Some(call) = parse_call(&item) else {
                        continue;
                    };
                    let state = &mut self.relations.aux.rollout;
                    let key = (thread.to_owned(), call.call_id);
                    let observed = RolloutCall {
                        epoch,
                        at: now,
                        wall_ms: entry.wall_ms,
                        trigger: call.trigger,
                        target: call.target,
                        message: call.message,
                    };
                    if !state.calls.contains_key(&key) && state.calls.len() < MAX_OPERATIONS {
                        state.calls.insert(key, observed);
                    } else if !state.calls.contains_key(&key) {
                        let mut taken = false;
                        for (operation, wait) in &mut state.waits {
                            if operation.parent_thread_id == key.0 && operation.item_id == key.1 {
                                wait.call.get_or_insert_with(|| observed.clone());
                                taken = true;
                            }
                        }
                        // 既放不进缓存又没有同键等待认领：在父线程上留失记标志，
                        // 之后到期的等待仍没拿到自己的调用时判未证明而不是宽松回退。
                        if !taken && state.lost_calls.len() < MAX_ROLLOUT_FILES {
                            state.lost_calls.insert(
                                key.0.clone(),
                                (epoch, now.saturating_add(EVIDENCE_TTL_MS)),
                            );
                        }
                    }
                }
                RolloutRecord::Message { turn, item } => {
                    // 有 raw 或能力未知的线程只认 raw。
                    if raw {
                        continue;
                    }
                    if entry.wall_ms < floor {
                        if let Some(file) = self.relations.aux.rollout.files.get_mut(thread) {
                            file.floor_dropped = file.floor_dropped.max(Some(entry.wall_ms));
                        }
                        continue;
                    }
                    let Some(delivery) = parse_delivery(&item) else {
                        continue;
                    };
                    // 轮次不明的记录对应不到任何轮。起点所在的轮定位不到（末尾窗口找到上限仍没有标记）时，
                    // 它可能正是要等的消费记录：不能静默丢掉，这个文件明确降级，等它的操作按宽松规则配对。
                    let Some(turn) = turn else {
                        turn_lost |= entry.turn_lost;
                        continue;
                    };
                    self.prune_v2_deliveries();
                    self.v2_record_delivery(
                        thread,
                        &turn,
                        delivery,
                        EvidenceSource::RolloutFile,
                        entry.wall_ms,
                        actions,
                    );
                }
            }
        }
        if turn_lost {
            if let Some(file) = self.relations.aux.rollout.files.get_mut(thread) {
                file.path = RolloutPathState::Failed(V2FallbackReason::RolloutTurnUnknown);
                file.next_at = now.saturating_add(ROLLOUT_RETRY_MS);
                file.cursor = None;
            }
        }
    }

    /// 接收方文件里的消息至少要晚到这里才可能属于等它的操作。
    ///
    /// 还没有操作在等它时（例如它先作为父线程文件被读来找调用），取读取时刻往前同样的余量：
    /// 父子事件由不同监听器送来，跟进它的 Interacted 可能晚于它的消费记录被读到；游标已经越过的
    /// 近期消息不能就此丢掉。更早的旧消息照旧不登记。
    fn rollout_floor(&self, thread: &str) -> u64 {
        let fallback = self.clock.now_ms.saturating_sub(ROLLOUT_FLOOR_SLACK_MS);
        self.rollout_waiting_floor(thread)
            .map_or(fallback, |floor| floor.min(fallback))
    }

    fn rollout_waiting_floor(&self, thread: &str) -> Option<u64> {
        let waits = self
            .relations
            .aux
            .rollout
            .waits
            .values()
            .filter(|w| w.child == thread)
            .map(|w| w.wall_ms);
        let strict = self
            .relations
            .operations
            .values()
            .filter(|op| self.rollout_strict_waiting(op) && op.receivers.contains(thread))
            .filter_map(|op| self.v2_start(op).map(|s| s.wall_ms));
        waits
            .chain(strict)
            .min()
            .map(|w| w.saturating_sub(ROLLOUT_FLOOR_SLACK_MS))
    }
}
