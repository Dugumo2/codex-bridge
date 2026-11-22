//! 静态树日志：只保存规范化摘要，不重新驱动轮次、诊断、预算或空转状态。
use super::{state::schedule, Phase, RunTimer, Runs};
use crate::claim::{Action, Machine};
use crate::relations::{
    BindingStatus, EvidenceOperation, EvidenceOutcome, MemberChange, RelationKind,
};
use crate::store::StoreOp;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::VecDeque;

const PER_THREAD_BYTES: usize = 64 * 1024;
const GLOBAL_BYTES: usize = 1024 * 1024;
const PENDING_MS: u64 = 30_000;
/// 单调时钟限定每条摘要的总等待时间，归属证据不会延长此期限。
const MAX_PENDING_MS: u64 = 120_000;
/// 已结算备忘的兜底时长：超出后不再补写确认版；已落盘的未确认版不受影响。
const SETTLED_MS: u64 = 600_000;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct EventLog {
    sequence: u64,
    /// 每条暂存摘要的稳定身份序号；summaryId 为 "{epoch}:s{n}"，补写确认版沿用同一个号。
    accepted: u64,
    last_timestamp: u64,
    pending: VecDeque<Pending>,
    bytes: usize,
    /// 已按未确认结算、仍等迟到确认的备忘；与暂存队列分开记账，有界。
    settled: VecDeque<SettledMemo>,
    settled_bytes: usize,
    timer: Option<u64>,
    timer_deadline: Option<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogNote {
    Unconfirmed,
    DiscoveryIncomplete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Summary {
    event: String,
    text: String,
    truncated: bool,
    item: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Pending {
    thread: String,
    turn: String,
    now: u64,
    expires: u64,
    summary: Summary,
    /// 接纳序号；summaryId 的稳定部分，补写确认版沿用。
    seq: u64,
    candidates: Vec<Candidate>,
    bytes: usize,
    deadline: u64,
}

/// 已按归属未确认结算的摘要备忘：迟到确认绑定到某个运行时，按它补写确认版。
/// 有界：总字节不超过 GLOBAL_BYTES（超限淘汰最旧），且每条只留 SETTLED_MS。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct SettledMemo {
    thread: String,
    turn: String,
    seq: u64,
    /// 原观察时间；补写行沿用。
    now: u64,
    summary: Summary,
    /// 收到未确认副本且结算时仍在跑的候选运行；只剩已结束的运行时不留备忘。
    runs: Vec<String>,
    bytes: usize,
    /// 单调时钟的兜底期限。
    expires: u64,
}

#[derive(Clone, Copy)]
enum Settlement<'a> {
    /// 超限接纳：结算后留下备忘等迟到确认。
    Limit,
    /// 关闭清理：结算但不留备忘。
    All,
    /// 运行结束：结算但不留备忘。
    Ending(&'a str),
    /// 暂存到期：结算后留下备忘等迟到确认。
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct Candidate {
    run: String,
    parent: String,
}

fn clean(text: &str, max: usize) -> (String, bool) {
    (
        text.chars()
            .take(max)
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect(),
        text.chars().count() > max,
    )
}

impl Runs {
    pub(crate) fn log(
        &mut self,
        run: &str,
        now: u64,
        event: &str,
        content: &str,
        actions: &mut Vec<Action>,
    ) {
        let Some(record) = self.runs.get(run) else {
            return;
        };
        let thread = record.thread_id.clone();
        self.log_owned(
            run, &thread, None, None, now, event, content, false, actions,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn log_owned(
        &mut self,
        run: &str,
        thread: &str,
        turn: Option<&str>,
        item: Option<&str>,
        now: u64,
        event: &str,
        content: &str,
        truncated: bool,
        actions: &mut Vec<Action>,
    ) {
        let Some(record) = self.runs.get(run) else {
            return;
        };
        let role = if record.thread_id == thread {
            "run"
        } else {
            "execution"
        };
        let parent = (role == "execution").then(|| record.thread_id.clone());
        self.log_source(
            run,
            thread,
            parent.as_deref(),
            role,
            turn,
            item,
            now,
            event,
            content,
            truncated,
            false,
            None,
            None,
            actions,
        );
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn log_source(
        &mut self,
        run: &str,
        thread: &str,
        parent: Option<&str>,
        role: &str,
        turn: Option<&str>,
        item: Option<&str>,
        now: u64,
        event: &str,
        content: &str,
        truncated: bool,
        late: bool,
        note: Option<LogNote>,
        summary_seq: Option<u64>,
        actions: &mut Vec<Action>,
    ) {
        if self.phase != Phase::Open {
            return;
        }
        let Some(record) = self.runs.get(run) else {
            return;
        };
        let (summary, capped) = clean(content, 240);
        self.event_log.sequence = self
            .event_log
            .sequence
            .checked_add(1)
            .expect("日志序号已耗尽");
        let short = self
            .index
            .threads
            .values()
            .find(|entry| entry.thread_id == thread)
            .map(|entry| entry.short_id.as_str());
        let mut payload = json!({
            "eventId": format!("{}:{}", self.epoch, self.event_log.sequence),
            "rootThreadId": record.thread_id, "threadId": thread,
            "parentThreadId": parent, "shortId": short, "sourceRole": role,
            "turnId": turn, "itemId": item, "summary": summary, "truncated": truncated || capped,
        });
        if let Some(seq) = summary_seq {
            payload["summaryId"] = json!(format!("{}:s{seq}", self.epoch));
        }
        // 归属未确认的行永不标 late：它按原观察时间结算，不表示迟到写入。
        if note != Some(LogNote::Unconfirmed) && (late || now < self.event_log.last_timestamp) {
            payload["late"] = json!(true);
        }
        match note {
            Some(LogNote::Unconfirmed) => payload["attribution"] = json!("unconfirmed"),
            Some(LogNote::DiscoveryIncomplete) => {
                payload["coverageIssue"] = json!("discoveryIncomplete");
            }
            None => {}
        }
        self.event_log.last_timestamp = self.event_log.last_timestamp.max(now);
        actions.push(Action::Store(StoreOp::AppendLog {
            short_id: record.short_id.clone(),
            lines: vec![format!(
                // 旧 writer 将正文控制字符替换为空格；实际 TAB 是不与旧正文碰撞的格式边界。
                "{} {run} {event} \t@cxb2 {payload}",
                super::utc_timestamp(now)
            )],
        }));
    }
}

fn summaries(
    thread: &str,
    turn: &str,
    method: &str,
    params: &Option<Value>,
    now: u64,
) -> Vec<Summary> {
    let Some(params) = params.as_ref() else {
        return vec![];
    };
    let mut out = Vec::new();
    match method {
        "turn/started" | "turn/completed" => {
            let text = if method == "turn/started" {
                turn
            } else {
                params["turn"]["status"].as_str().unwrap_or("unknown")
            };
            let (text, truncated) = clean(text, 240);
            out.push(Summary {
                event: if method == "turn/started" {
                    "轮次开始"
                } else {
                    "轮次完成"
                }
                .into(),
                text,
                truncated,
                item: None,
            });
        }
        "item/started" | "item/completed" => {
            if let Some((item, truncated)) = params
                .get("item")
                .and_then(crate::results::summarize_item_with_truncation)
            {
                out.push(Summary {
                    event: if method == "item/started" {
                        "条目开始"
                    } else {
                        "条目完成"
                    }
                    .into(),
                    text: item.summary,
                    truncated,
                    item: Some(item.item_id),
                });
            }
        }
        _ => {}
    }
    let error = match method {
        "error" => params.get("error").filter(|e| e.is_object()).map(|error| {
            (
                error,
                params["willRetry"].as_bool(),
                super::upstream_error::UpstreamErrorSource::ErrorNotification,
            )
        }),
        "turn/completed" if params["turn"]["status"] == "failed" => params["turn"]
            .get("error")
            .filter(|e| e.is_object())
            .map(|error| {
                (
                    error,
                    Some(false),
                    super::upstream_error::UpstreamErrorSource::TurnCompleted,
                )
            }),
        _ => None,
    };
    if let Some((error, retry, source)) = error {
        let error = super::upstream_error::UpstreamError::from_error(
            thread,
            Some(turn),
            source,
            error,
            retry,
            now,
        );
        let (text, truncated) = clean(
            &format!(
                "willRetry={}；{}",
                retry.map_or("unknown", |r| if r { "true" } else { "false" }),
                error.message
            ),
            240,
        );
        out.push(Summary {
            event: "子线程错误".into(),
            text,
            truncated: truncated || error.truncated,
            item: None,
        });
    }
    out
}

impl Machine {
    /// 本入口只由完成首次接纳之后调用；不依赖进展是否推进。
    pub(crate) fn log_bound_child_event(
        &mut self,
        run: &str,
        thread: &str,
        turn: &str,
        method: &str,
        params: &Option<Value>,
        actions: &mut Vec<Action>,
    ) {
        let Some(binding) = self
            .binding(thread, turn)
            .filter(|b| b.run_id == run && b.status == BindingStatus::Bound)
        else {
            return;
        };
        for summary in summaries(thread, turn, method, params, self.clock.now_ms) {
            self.write_child_summary(
                run,
                thread,
                &binding.parent_thread_id,
                turn,
                self.clock.now_ms,
                &summary,
                false,
                None,
                None,
                actions,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn write_child_summary(
        &mut self,
        run: &str,
        thread: &str,
        parent: &str,
        turn: &str,
        now: u64,
        summary: &Summary,
        late: bool,
        note: Option<LogNote>,
        summary_seq: Option<u64>,
        actions: &mut Vec<Action>,
    ) {
        self.runs.log_source(
            run,
            thread,
            Some(parent),
            "subagent",
            Some(turn),
            summary.item.as_deref(),
            now,
            &summary.event,
            &summary.text,
            summary.truncated,
            late,
            note,
            summary_seq,
            actions,
        );
    }

    /// 只为关系层已知候选保留摘要；独立认领或明确冲突永远不暂存。
    pub(crate) fn buffer_child_event_log(
        &mut self,
        thread: &str,
        method: &str,
        params: &Option<Value>,
        replay: bool,
        actions: &mut Vec<Action>,
    ) {
        if replay
            || self.runs.phase != Phase::Open
            || self.claims.contains_key(thread)
            || !matches!(
                method,
                "turn/started" | "turn/completed" | "item/started" | "item/completed" | "error"
            )
        {
            return;
        }
        let Some(turn) = params.as_ref().and_then(|p| {
            p.get("turnId").and_then(Value::as_str).or_else(|| {
                p.get("turn")
                    .and_then(|t| t.get("id"))
                    .and_then(Value::as_str)
            })
        }) else {
            return;
        };
        if self
            .binding(thread, turn)
            .is_some_and(|b| b.status != BindingStatus::Unbound)
        {
            return;
        }
        if self
            .runs
            .active
            .keys()
            .filter_map(|run| self.runs.runs.get(run))
            .any(|r| r.thread_id == thread || r.review_thread_id.as_deref() == Some(thread))
        {
            return;
        }
        if method == "turn/completed"
            && self
                .records
                .get(thread)
                .is_some_and(|r| r.ended_turns.iter().any(|t| t == turn))
        {
            return;
        }
        let candidates = self.event_log_candidates(thread);
        if candidates.is_empty() {
            return;
        }
        for summary in summaries(thread, turn, method, params, self.clock.now_ms) {
            let seq = self
                .runs
                .event_log
                .accepted
                .checked_add(1)
                .expect("摘要序号已耗尽");
            self.runs.event_log.accepted = seq;
            let mut pending = Pending {
                thread: thread.into(),
                turn: turn.into(),
                now: self.clock.now_ms,
                expires: self.clock.mono_ms.saturating_add(PENDING_MS),
                summary,
                seq,
                candidates: candidates.clone(),
                bytes: 0,
                deadline: self.clock.mono_ms.saturating_add(MAX_PENDING_MS),
            };
            pending.bytes = serde_json::to_vec(&pending).expect("摘要可序列化").len();
            let thread_bytes: usize = self
                .runs
                .event_log
                .pending
                .iter()
                .filter(|p| p.thread == thread)
                .map(|p| p.bytes)
                .sum();
            if pending.bytes + thread_bytes > PER_THREAD_BYTES
                || pending.bytes + self.runs.event_log.bytes > GLOBAL_BYTES
            {
                self.settle_pending_candidates(&mut pending, Settlement::Limit, actions);
                continue;
            }
            self.runs.event_log.bytes += pending.bytes;
            self.runs.event_log.pending.push_back(pending);
        }
        self.schedule_event_log(actions);
    }

    fn event_log_candidates(&self, thread: &str) -> Vec<Candidate> {
        let mut candidates = std::collections::BTreeMap::new();
        // 祖先尚未绑定时，后代操作的 run_id 为空。沿已知血缘与候选操作继续上溯；
        // 保存的 parent 始终是事件线程的直接父线程，不能改成找到运行的祖先。
        // 访问集防止畸形环。候选只决定摘要存放位置，不能授予轮次管理权。
        let mut pending = vec![(thread.to_owned(), None::<String>)];
        let mut visited = std::collections::BTreeSet::new();
        while let Some((current, direct_parent)) = pending.pop() {
            if !visited.insert((current.clone(), direct_parent.clone())) {
                continue;
            }
            for (run, context) in &self.relations.contexts {
                if let Some(member) = context
                    .members
                    .get(&current)
                    .filter(|m| m.kind == RelationKind::Subagent && m.conflict.is_none())
                {
                    candidates.insert(
                        run.clone(),
                        direct_parent.as_ref().unwrap_or(&member.parent).clone(),
                    );
                }
                if context.identity.execution_thread_id == current {
                    if let Some(parent) = &direct_parent {
                        candidates.insert(run.clone(), parent.clone());
                    }
                }
            }
            // 独立认领是新的执行根，不借其旧血缘继续向另一个运行追溯。
            if self.claims.contains_key(&current) {
                continue;
            }
            for op in self.relations.operations.values().filter(|op| {
                op.receivers.contains(&current) && !op.rejected_receivers.contains_key(&current)
            }) {
                let parent = direct_parent
                    .as_ref()
                    .unwrap_or(&op.key.parent_thread_id)
                    .clone();
                if self.runs.runs.contains_key(&op.run_id) {
                    candidates.insert(op.run_id.clone(), parent);
                } else if op.run_id.is_empty() {
                    pending.push((op.key.parent_thread_id.clone(), Some(parent)));
                }
            }
            if let Some(edge) = self
                .thread_relation(&current)
                .filter(|e| e.kind == RelationKind::Subagent)
            {
                let parent = direct_parent.unwrap_or_else(|| edge.parent_thread_id.clone());
                pending.push((edge.parent_thread_id, Some(parent)));
            }
        }
        candidates
            .into_iter()
            .filter(|(run, _)| {
                self.runs.runs.get(run).is_some_and(|r| {
                    matches!(
                        r.state,
                        super::RunState::Running | super::RunState::Stopping
                    )
                })
            })
            .map(|(run, parent)| Candidate { run, parent })
            .collect()
    }

    /// 关系证据确认后仅补写摘要；已经结束的轮次也不重放通知。
    pub(crate) fn flush_bound_event_log(
        &mut self,
        run: &str,
        thread: &str,
        turn: &str,
        parent: &str,
        actions: &mut Vec<Action>,
    ) {
        if !self.binding(thread, turn).is_some_and(|b| {
            b.run_id == run && b.status == BindingStatus::Bound && b.parent_thread_id == parent
        }) {
            return;
        }
        let mut hits: Vec<(u64, u64, Summary)> = vec![];
        for event in std::mem::take(&mut self.runs.event_log.pending) {
            if event.thread == thread && event.turn == turn {
                self.runs.event_log.bytes -= event.bytes;
                hits.push((event.seq, event.now, event.summary));
            } else {
                self.runs.event_log.pending.push_back(event);
            }
        }
        // 已结算备忘：超期的丢掉，匹配的摘出来与暂存一起按接纳序号补写。
        let mut settled_bytes = self.runs.event_log.settled_bytes;
        let mut settled = std::mem::take(&mut self.runs.event_log.settled);
        settled.retain(|memo| {
            if memo.expires <= self.clock.mono_ms {
                settled_bytes -= memo.bytes;
                return false;
            }
            if memo.thread == thread && memo.turn == turn {
                settled_bytes -= memo.bytes;
                hits.push((memo.seq, memo.now, memo.summary.clone()));
                return false;
            }
            true
        });
        self.runs.event_log.settled = settled;
        self.runs.event_log.settled_bytes = settled_bytes;
        hits.sort_by_key(|(seq, ..)| *seq);
        for (seq, now, summary) in hits {
            self.write_child_summary(
                run,
                thread,
                parent,
                turn,
                now,
                &summary,
                true,
                None,
                Some(seq),
                actions,
            );
        }
    }

    /// 已绑定的轮只写给实际运行。未绑定时按候选分别结算，保留其直接父线程。
    fn settle_pending_candidates(
        &mut self,
        event: &mut Pending,
        when: Settlement<'_>,
        actions: &mut Vec<Action>,
    ) {
        if let Some(binding) = self
            .binding(&event.thread, &event.turn)
            .filter(|b| b.status == BindingStatus::Bound)
        {
            self.write_child_summary(
                &binding.run_id,
                &event.thread,
                &binding.parent_thread_id,
                &event.turn,
                event.now,
                &event.summary,
                true,
                None,
                Some(event.seq),
                actions,
            );
            event.candidates.clear();
            return;
        }
        let mut written: Vec<String> = vec![];
        event.candidates.retain(|candidate| {
            let Candidate { run, parent } = candidate;
            match when {
                Settlement::Ending(ending) if ending != run => return true,
                Settlement::Expired => {
                    let evidence_wait = self.runs.runs.get(run).is_some_and(|r| {
                        matches!(
                            r.state,
                            super::RunState::Running | super::RunState::Stopping
                        )
                    }) && self.event_log_evidence_in_flight(run, &event.thread);
                    if evidence_wait && self.clock.mono_ms < event.deadline {
                        return true;
                    }
                }
                _ => {}
            }
            // 历史子代理同样按归属未确认保存；归属排除已移除，结算只负责落盘。
            self.write_child_summary(
                run,
                &event.thread,
                parent,
                &event.turn,
                event.now,
                &event.summary,
                false,
                Some(LogNote::Unconfirmed),
                Some(event.seq),
                actions,
            );
            written.push(run.clone());
            false
        });
        // 到期或超限结算的摘要留下备忘，迟到确认时补写确认版；运行结束或关闭不留。
        if event.candidates.is_empty() && matches!(when, Settlement::Expired | Settlement::Limit) {
            let live: Vec<String> = written
                .into_iter()
                .filter(|run| {
                    self.runs.runs.get(run).is_some_and(|r| {
                        matches!(
                            r.state,
                            super::RunState::Running | super::RunState::Stopping
                        )
                    })
                })
                .collect();
            if !live.is_empty() {
                let mut memo = SettledMemo {
                    thread: event.thread.clone(),
                    turn: event.turn.clone(),
                    seq: event.seq,
                    now: event.now,
                    summary: event.summary.clone(),
                    runs: live,
                    bytes: 0,
                    expires: self.clock.mono_ms.saturating_add(SETTLED_MS),
                };
                memo.bytes = serde_json::to_vec(&memo).expect("备忘可序列化").len();
                while memo.bytes + self.runs.event_log.settled_bytes > GLOBAL_BYTES {
                    match self.runs.event_log.settled.pop_front() {
                        Some(oldest) => self.runs.event_log.settled_bytes -= oldest.bytes,
                        None => break,
                    }
                }
                if memo.bytes <= GLOBAL_BYTES {
                    self.runs.event_log.settled_bytes += memo.bytes;
                    self.runs.event_log.settled.push_back(memo);
                }
            }
        }
    }

    /// 其他候选仍能在之后绑定。字节数按最初接纳的大小保守记账，直到最后一个候选结算。
    pub(super) fn settle_ending_event_log(&mut self, run: &str, actions: &mut Vec<Action>) {
        for mut event in std::mem::take(&mut self.runs.event_log.pending) {
            if event
                .candidates
                .iter()
                .any(|candidate| candidate.run == run)
            {
                self.settle_pending_candidates(&mut event, Settlement::Ending(run), actions);
            }
            if event.candidates.is_empty() {
                self.runs.event_log.bytes -= event.bytes;
            } else {
                self.runs.event_log.pending.push_back(event);
            }
        }
        // 已结束的运行不会再确认：只属于它的备忘丢掉。
        let mut kept = VecDeque::new();
        for mut memo in std::mem::take(&mut self.runs.event_log.settled) {
            memo.runs.retain(|run_id| *run_id != run);
            if memo.runs.is_empty() {
                self.runs.event_log.settled_bytes -= memo.bytes;
            } else {
                kept.push_back(memo);
            }
        }
        self.runs.event_log.settled = kept;
        self.schedule_event_log(actions);
    }

    /// 还在进行、可能把这个线程的轮次归给该运行的证据：等配对或等发起方绑定的跟进、
    /// 等会话文件的跟进、等首轮的派生。多数有自己的证据期限，到期转成结果或未证明；
    /// 等 v2 配对的不受证据期限约束（`evidence_expirable`），靠父轮结束、收敛总期限和运行结束收口。
    fn event_log_evidence_in_flight(&self, run: &str, thread: &str) -> bool {
        let applicable = |op: &EvidenceOperation| {
            op.outcome == EvidenceOutcome::Pending
                && (op.run_id == run || op.run_id.is_empty())
                && !op.rejected_receivers.contains_key(thread)
        };
        let waits = self
            .relations
            .aux
            .rollout
            .waits
            .iter()
            .filter_map(|(key, wait)| (wait.child == thread).then_some(key))
            .chain(self.relations.aux.spawn_waits.get(thread));
        self.relations
            .operations
            .values()
            .any(|op| op.receivers.contains(thread) && applicable(op))
            || waits
                .filter_map(|key| self.relations.operations.get(key))
                .any(applicable)
    }

    /// 在途归属证据可续期；原到达顺序和独立截止时间均保持不变。
    fn expire_event_log(&mut self, actions: &mut Vec<Action>) {
        let pending = std::mem::take(&mut self.runs.event_log.pending);
        for mut event in pending {
            if event.expires <= self.clock.mono_ms {
                self.settle_pending_candidates(&mut event, Settlement::Expired, actions);
                if event.candidates.is_empty() {
                    self.runs.event_log.bytes -= event.bytes;
                    continue;
                }
                event.expires = self
                    .clock
                    .mono_ms
                    .saturating_add(PENDING_MS)
                    .min(event.deadline);
            }
            self.runs.event_log.pending.push_back(event);
        }
        // 备忘兜底期限到了就不再等确认。
        let mut settled_bytes = self.runs.event_log.settled_bytes;
        self.runs.event_log.settled.retain(|memo| {
            if memo.expires <= self.clock.mono_ms {
                settled_bytes -= memo.bytes;
                false
            } else {
                true
            }
        });
        self.runs.event_log.settled_bytes = settled_bytes;
    }

    pub(crate) fn settle_event_log_step(&mut self, actions: &mut Vec<Action>) {
        self.expire_event_log(actions);
        self.schedule_event_log(actions);
    }
    fn schedule_event_log(&mut self, actions: &mut Vec<Action>) {
        let deadline = self.runs.event_log.pending.iter().map(|p| p.expires).min();
        if self.runs.event_log.timer.is_some() && self.runs.event_log.timer_deadline == deadline {
            return;
        }
        self.cancel_event_log_timer(actions);
        if let Some(deadline) = deadline {
            self.runs.event_log.timer_deadline = Some(deadline);
            let generation = self.runs.generation();
            self.runs.event_log.timer = Some(generation);
            schedule(
                "",
                RunTimer::EventLogSweep { generation },
                deadline.saturating_sub(self.clock.mono_ms),
                actions,
            );
        }
    }

    pub(crate) fn sweep_event_log(&mut self, generation: u64) {
        if self.runs.event_log.timer != Some(generation) {
            return;
        }
        self.runs.event_log.timer = None;
        self.runs.event_log.timer_deadline = None;
    }

    pub(crate) fn clear_pending_event_log(&mut self, actions: &mut Vec<Action>) {
        for mut pending in std::mem::take(&mut self.runs.event_log.pending) {
            self.settle_pending_candidates(&mut pending, Settlement::All, actions);
        }
        self.runs.event_log.bytes = 0;
        // 关闭清理不留备忘。
        self.runs.event_log.settled.clear();
        self.runs.event_log.settled_bytes = 0;
        self.cancel_event_log_timer(actions);
    }

    fn cancel_event_log_timer(&mut self, actions: &mut Vec<Action>) {
        self.runs.event_log.timer_deadline = None;
        if self.runs.event_log.timer.take().is_some() {
            actions.push(Action::Unschedule {
                key: crate::claim::TimerKey::EventLogSweep,
            });
        }
    }

    pub(crate) fn log_member_change(
        &mut self,
        run: &str,
        thread: &str,
        _change: MemberChange,
        actions: &mut Vec<Action>,
    ) {
        let Some(context) = self.relations.contexts.get(run) else {
            return;
        };
        if let Some(member) = context
            .members
            .get(thread)
            .filter(|m| !m.bindings.is_empty())
        {
            self.runs.log_source(
                run,
                thread,
                Some(&member.parent),
                "subagent",
                None,
                None,
                self.clock.now_ms,
                "成员状态变化",
                &format!("{:?}", member.activity),
                false,
                false,
                None,
                None,
                actions,
            );
        }
    }

    /// 只在运行定案时保存发现缺口；分类失效和重新发现期间的状态不代表覆盖丢失。
    pub(crate) fn log_final_discovery(
        &mut self,
        run: &str,
        complete: bool,
        actions: &mut Vec<Action>,
    ) {
        if complete {
            return;
        }
        let Some(record) = self.runs.runs.get(run) else {
            return;
        };
        let root = record.thread_id.clone();
        self.runs.log_source(
            run,
            &root,
            None,
            "run",
            None,
            None,
            self.clock.now_ms,
            "记录缺口",
            "运行定案时子代理发现范围仍不完整",
            false,
            false,
            Some(LogNote::DiscoveryIncomplete),
            None,
            actions,
        );
    }
}

#[cfg(test)]
mod tests;
