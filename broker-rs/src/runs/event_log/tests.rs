use super::*;
use crate::claim::ReqKind;
use crate::relations::*;
use crate::runs::{RunOpen, RunState};

fn setup() -> Machine {
    let mut m = Machine::new(false);
    m.clock.now_ms = 1000;
    m.runs.epoch = 42;
    open(&mut m, "run-a", "root");
    m
}

fn open(m: &mut Machine, run: &str, root: &str) {
    m.run_opened(
        RunOpen {
            run_id: run.into(),
            thread_id: root.into(),
            conn: None,
            kind: ReqKind::TurnStart,
            joined: false,
            input: String::new(),
            continued_from: None,
        },
        &mut Vec::new(),
    );
    m.relations.contexts.insert(
        run.into(),
        ManageContext {
            identity: RunIdentity {
                run_id: run.into(),
                logical_root_thread_id: root.into(),
                execution_thread_id: root.into(),
            },
            gen: 1,
            authority: Authority::Owner(1),
            policy: crate::policy::Policy::OnRequest,
            members: Default::default(),
            in_flight_spawns: Default::default(),
            revision: 0,
            discovery: DiscoveryState {
                missing: Default::default(),
                complete: true,
                version: 0,
                incomplete_reasons: vec![],
            },
            cleanup: None,
        },
    );
}

fn candidate(m: &mut Machine, thread: &str, parent: &str) {
    m.relations.edges.insert(
        thread.into(),
        RelationEdge {
            child_thread_id: thread.into(),
            parent_thread_id: parent.into(),
            kind: RelationKind::Subagent,
            source_summary: Value::Null,
            can_accept_direct_input: Some(false),
            agent_version: AgentVersion::V1,
            discovered_by: DiscoveredBy::Started,
        },
    );
}

fn bind(m: &mut Machine, run: &str, thread: &str, turn: &str, parent: &str) {
    candidate(m, thread, parent);
    m.relations.bindings.insert(
        (thread.into(), turn.into()),
        ActivityBinding {
            thread_id: thread.into(),
            turn_id: turn.into(),
            run_id: run.into(),
            parent_thread_id: parent.into(),
            evidence: BindingEvidence::V1Spawn,
            operation: EvidenceKey {
                parent_thread_id: parent.into(),
                parent_turn_id: "parent-turn".into(),
                item_id: "spawn".into(),
            },
            status: BindingStatus::Bound,
        },
    );
    m.relations
        .aux
        .observed
        .entry(thread.into())
        .or_default()
        .push_back(TurnObservation {
            turn: turn.into(),
            started: true,
            completed: false,
            seen_ms: m.clock.mono_ms,
        });
}

fn logs(actions: &[Action]) -> Vec<(String, String, String, Value)> {
    actions
        .iter()
        .flat_map(|a| match a {
            Action::Store(StoreOp::AppendLog { short_id, lines }) => lines
                .iter()
                .map(|line| {
                    let fields: Vec<_> = line.splitn(4, ' ').collect();
                    (
                        short_id.clone(),
                        fields[1].into(),
                        fields[2].into(),
                        serde_json::from_str(
                            fields[3].strip_prefix("\t@cxb2 ").expect("结构化标记"),
                        )
                        .expect("完整 JSON"),
                    )
                })
                .collect(),
            _ => vec![],
        })
        .collect()
}

fn item(thread: &str, turn: &str) -> Option<Value> {
    Some(
        json!({"threadId":thread,"turnId":turn,"item":{"id":"same-item","type":"agentMessage","text":"同毫秒同正文"}}),
    )
}

#[test]
fn tree_log_实际tab边界与正文标志不会混淆() {
    let mut m = setup();
    let mut actions = vec![];
    m.runs.log(
        "run-a",
        1000,
        "轮次开始",
        "@cxb2 正文\t控制字符\n仍是摘要",
        &mut actions,
    );
    let lines = logs(&actions);
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].3["summary"], "@cxb2 正文 控制字符 仍是摘要");
    let Action::Store(StoreOp::AppendLog { lines, .. }) = &actions[0] else {
        panic!("应写入日志");
    };
    assert_eq!(lines[0].chars().filter(|c| *c == '\t').count(), 1);
    assert!(lines[0].contains(" 轮次开始 \t@cxb2 {"));
}

#[test]
fn tree_log_发现中间态与候选分类不写缺口_恢复完整后结束零误报() {
    let mut m = setup();
    let mut actions = vec![];
    candidate(&mut m, "child", "root");
    m.refresh_classification("run-a", "child", "Discovery", &mut actions);
    assert!(!m.manage_context("run-a").unwrap().discovery.complete);
    m.log_member_change("run-a", "child", MemberChange::Discovery, &mut actions);
    assert!(logs(&actions).iter().any(|l| l.2 == "候选分类"));
    assert!(logs(&actions)
        .iter()
        .all(|l| l.3.get("coverageIssue").is_none()));
    m.relations
        .contexts
        .get_mut("run-a")
        .unwrap()
        .discovery
        .complete = true;
    m.run_released("run-a", crate::runs::ReleaseKind::Natural, &mut actions);
    assert_eq!(m.runs.runs["run-a"].state, RunState::Ended);
    assert!(logs(&actions).iter().all(|l| l.2 != "记录缺口"));
}

#[test]
fn gap_wu_无候选且发现失败只写运行级缺口() {
    for shutdown in [false, true] {
        let mut m = setup();
        let state = &mut m.relations.contexts.get_mut("run-a").unwrap().discovery;
        state.complete = false;
        state.version = 1;
        state.incomplete_reasons.push("listFailed".into());
        let mut actions = vec![];
        m.buffer_child_event_log(
            "未知",
            "item/completed",
            &item("未知", "虚构轮"),
            false,
            &mut actions,
        );
        assert!(m.runs.event_log.pending.is_empty());
        if shutdown {
            m.shutdown_runs(&mut actions);
            m.shutdown_runs(&mut actions);
        } else {
            m.run_released("run-a", crate::runs::ReleaseKind::Natural, &mut actions);
            m.run_released("run-a", crate::runs::ReleaseKind::Natural, &mut actions);
        }
        let gaps: Vec<_> = logs(&actions)
            .into_iter()
            .filter(|l| l.3["coverageIssue"] == "discoveryIncomplete")
            .collect();
        assert_eq!(gaps.len(), 1, "每次定案只记录一次真实缺口，关闭={shutdown}");
        assert_eq!(gaps[0].2, "记录缺口");
        assert_eq!(gaps[0].3["threadId"], "root");
        assert!(gaps[0].3["turnId"].is_null());
        assert!(logs(&actions).iter().all(|l| l.3["threadId"] != "未知"));
    }
}

#[test]
fn tree_log_根子孙同文件身份与同正文独立事件以及review来源() {
    let mut m = setup();
    bind(&mut m, "run-a", "child", "ct", "root");
    bind(&mut m, "run-a", "grandchild", "gt", "child");
    let mut a = vec![];
    m.runs.log("run-a", 1000, "运行开始", "开始", &mut a);
    m.log_bound_child_event(
        "run-a",
        "child",
        "ct",
        "item/completed",
        &item("child", "ct"),
        &mut a,
    );
    m.log_bound_child_event(
        "run-a",
        "grandchild",
        "gt",
        "item/completed",
        &item("grandchild", "gt"),
        &mut a,
    );
    m.runs.runs.get_mut("run-a").unwrap().review_thread_id = Some("review".into());
    m.runs.log_owned(
        "run-a",
        "review",
        Some("rt"),
        Some("ri"),
        1000,
        "条目完成",
        "审查",
        false,
        &mut a,
    );
    let lines = logs(&a);
    assert_eq!(lines.len(), 4);
    assert!(lines.iter().all(|l| l.0 == lines[0].0 && l.1 == "run-a"));
    assert_eq!(
        lines
            .iter()
            .map(|l| l.3["threadId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["root", "child", "grandchild", "review"]
    );
    assert_eq!(lines[2].3["parentThreadId"], "child");
    assert_eq!(lines[3].3["sourceRole"], "execution");
    assert_eq!(lines[1].3["summary"], lines[2].3["summary"]);
    assert_ne!(lines[1].3["eventId"], lines[2].3["eventId"]);
}

#[test]
fn tree_log_逐轮逐运行校验不跨账也不接纳冲突() {
    let mut m = setup();
    open(&mut m, "run-b", "other-root");
    bind(&mut m, "run-a", "child", "old-turn", "root");
    bind(&mut m, "run-b", "child", "new-turn", "other-root");
    let mut a = vec![];
    m.log_bound_child_event(
        "run-a",
        "child",
        "new-turn",
        "item/completed",
        &item("child", "new-turn"),
        &mut a,
    );
    m.log_bound_child_event(
        "run-a",
        "child",
        "unknown-turn",
        "item/completed",
        &item("child", "unknown-turn"),
        &mut a,
    );
    assert!(logs(&a).is_empty());
    m.log_bound_child_event(
        "run-a",
        "child",
        "old-turn",
        "item/completed",
        &item("child", "old-turn"),
        &mut a,
    );
    m.log_bound_child_event(
        "run-b",
        "child",
        "new-turn",
        "item/completed",
        &item("child", "new-turn"),
        &mut a,
    );
    assert_eq!(
        logs(&a).iter().map(|l| l.1.as_str()).collect::<Vec<_>>(),
        ["run-a", "run-b"]
    );
    m.relations
        .bindings
        .get_mut(&("child".into(), "old-turn".into()))
        .unwrap()
        .status = BindingStatus::Conflict("独立认领".into());
    a.clear();
    m.log_bound_child_event(
        "run-a",
        "child",
        "old-turn",
        "item/completed",
        &item("child", "old-turn"),
        &mut a,
    );
    assert!(logs(&a).is_empty());
}

#[test]
fn tree_log_完成首次接纳且父轮预算空转进度不因日志改变() {
    let mut m = setup();
    bind(&mut m, "run-a", "child", "ct", "root");
    let mut a = vec![];
    let before = m.runs.runs["run-a"].clone();
    let guard = m.loop_guards.clone();
    let params =
        Some(json!({"threadId":"child","turn":{"id":"ct","status":"completed","items":[]}}));
    assert!(m.dispatch_child_diagnostic("child", "turn/completed", &params, false, &mut a));
    assert!(m.dispatch_child_diagnostic("child", "turn/completed", &params, false, &mut a));
    assert_eq!(logs(&a).iter().filter(|l| l.2 == "轮次完成").count(), 1);
    let after = &m.runs.runs["run-a"];
    assert_eq!(after.state, RunState::Running);
    assert_eq!(after.turns, before.turns);
    assert_eq!(after.turns_total, before.turns_total);
    assert_eq!(after.progress, before.progress);
    assert_eq!(after.run_budget, before.run_budget);
    assert_eq!(m.loop_guards, guard);
}

#[test]
fn tree_log_绑定前完成只补摘要不重放诊断与完成状态() {
    let mut m = setup();
    candidate(&mut m, "child", "root");
    let mut a = vec![];
    m.buffer_child_event_log(
        "child",
        "item/completed",
        &item("child", "ct"),
        false,
        &mut a,
    );
    let completed = Some(
        json!({"threadId":"child","turn":{"id":"ct","status":"failed","error":{"message":"Authorization: secret-value"}}}),
    );
    m.buffer_child_event_log("child", "turn/completed", &completed, false, &mut a);
    assert!(logs(&a).is_empty());
    m.records
        .entry("child".into())
        .or_default()
        .ended_turns
        .push_back("ct".into());
    bind(&mut m, "run-a", "child", "ct", "root");
    let before = m.runs.runs["run-a"].clone();
    let active = m.runs.active["run-a"].clone();
    m.clock.now_ms = 2000;
    m.flush_bound_event_log("run-a", "child", "ct", "root", &mut a);
    assert_eq!(m.runs.runs["run-a"], before);
    assert_eq!(m.runs.active["run-a"], active);
    assert_eq!(logs(&a).len(), 3);
    assert!(logs(&a).iter().all(|l| l.3["late"] == true));
    assert!(!format!("{a:?}").contains("secret-value"));
    assert!(m.runs.event_log.pending.is_empty());
    assert_eq!(m.runs.event_log.bytes, 0);
    a.clear();
    m.flush_bound_event_log("run-a", "child", "ct", "root", &mut a);
    assert!(logs(&a).is_empty());
}

#[test]
fn tree_log_全局暂存上限与未知线程不暂存() {
    let mut m = setup();
    let mut a = vec![];
    m.buffer_child_event_log(
        "unknown",
        "item/completed",
        &item("unknown", "t"),
        false,
        &mut a,
    );
    assert!(m.runs.event_log.pending.is_empty());
    for n in 0..25 {
        let thread = format!("child-{n}");
        candidate(&mut m, &thread, "root");
        for _ in 0..300 {
            m.buffer_child_event_log(
                &thread,
                "item/completed",
                &item(&thread, "t"),
                false,
                &mut a,
            );
        }
    }
    assert!(m.runs.event_log.bytes <= GLOBAL_BYTES);
    assert!(m.runs.event_log.bytes > GLOBAL_BYTES - 1024);
    assert!(logs(&a)
        .iter()
        .any(|l| l.3["attribution"] == "unconfirmed" && l.3["summary"] == "同毫秒同正文"));
}

#[test]
fn tree_log_摘要截断标记与长身份的json保持完整() {
    let mut m = setup();
    let thread = "子线程".repeat(200);
    let turn = "轮".repeat(300);
    bind(&mut m, "run-a", &thread, &turn, "root");
    for (text, truncated) in [("甲".repeat(80), false), ("乙".repeat(81), true)] {
        let mut a = vec![];
        let params = Some(
            json!({"threadId":thread,"turnId":turn,"item":{"id":"条目","type":"agentMessage","text":text}}),
        );
        m.log_bound_child_event("run-a", &thread, &turn, "item/completed", &params, &mut a);
        let lines = logs(&a);
        assert_eq!(lines[0].3["threadId"], thread);
        assert_eq!(lines[0].3["turnId"], turn);
        assert_eq!(lines[0].3["truncated"], truncated);
        assert_eq!(lines[0].3["summary"].as_str().unwrap().chars().count(), 80);
    }
}

fn evidence_to(
    m: &mut Machine,
    receiver: &str,
    kind: EvidenceKind,
    outcome: EvidenceOutcome,
    proof_expired: bool,
    rejected: bool,
) {
    let id = "条目";
    let key = EvidenceKey {
        parent_thread_id: "发起方".into(),
        parent_turn_id: "发起轮".into(),
        item_id: id.into(),
    };
    m.relations.operations.insert(
        key.clone(),
        EvidenceOperation {
            key,
            run_id: String::new(),
            conn_epoch: m.relations.conn_epoch,
            authority_gen: 0,
            kind,
            started_seen: true,
            completed_seen: true,
            outcome,
            receivers: [receiver.to_owned()].into(),
            rejected_receivers: if rejected {
                [(receiver.to_owned(), FollowupRejection::Peer)].into()
            } else {
                Default::default()
            },
            proven_turns: Default::default(),
            baseline_turns: Default::default(),
            v2_strict: false,
            v2_queue_only: false,
            v2_source: V2EvidenceSource::None,
            v2_fallback: None,
            rollout_arrival: None,
            proof_expired,
            lenient_floor: None,
            first_seen_ms: 0,
            expires_at_ms: 1,
            parent_ended: false,
            failure: None,
        },
    );
}
#[test]
fn gap_wu_历史线程的过期疑点保留原文() {
    use EvidenceOutcome::{Failed, Unproven};
    for (kind, outcome, expired, rejected, expect_saved) in [
        (EvidenceKind::V2Interacted, Unproven, true, false, true),
        (EvidenceKind::V2Interacted, Unproven, true, true, false),
        (EvidenceKind::V2Interacted, Unproven, false, false, false),
        (EvidenceKind::V2Completed, Unproven, false, false, false),
        (EvidenceKind::V1ResumeAgent, Failed, true, false, false),
    ] {
        let mut m = setup();
        candidate(&mut m, "历史线程", "root");
        m.runs.index.child_history = serde_json::from_value(json!({
            "coverage":"verified","retainedAfter":0,
            "entries":{"历史线程":{"run-old":{"rootThreadId":"root","observedAt":1}}}
        }))
        .unwrap();
        evidence_to(&mut m, "历史线程", kind, outcome, expired, rejected);
        assert!(m.historical_subagent_for_run("run-a", "历史线程"), "前置");
        let mut a = vec![];
        m.buffer_child_event_log(
            "历史线程",
            "item/completed",
            &item("历史线程", "t"),
            false,
            &mut a,
        );
        let generation = m.runs.event_log.timer.unwrap();
        m.clock.mono_ms = PENDING_MS;
        a.clear();
        m.sweep_event_log(generation);
        m.settle_event_log_step(&mut a);
        let rows = logs(&a);
        assert_eq!(rows.len(), usize::from(expect_saved));
        assert!(rows.iter().all(|l| l.2 == "条目完成"
            && l.3["attribution"] == "unconfirmed"
            && l.3["summary"] == "同毫秒同正文"));
    }
}

#[test]
fn tree_log_明确不归本运行的历史兄弟暂存超限不写缺口() {
    let mut m = setup();
    candidate(&mut m, "sibling", "root");
    candidate(&mut m, "unknown", "root");
    m.runs.index.child_history = serde_json::from_value(json!({
        "coverage":"verified","retainedAfter":0,
        "entries":{"sibling":{"run-old":{"rootThreadId":"root","observedAt":1}}}
    }))
    .unwrap();
    let mut a = vec![];
    for _ in 0..1000 {
        for thread in ["sibling", "unknown"] {
            m.buffer_child_event_log(thread, "item/completed", &item(thread, "t"), false, &mut a);
        }
    }
    let saved: std::collections::BTreeSet<String> = logs(&a)
        .into_iter()
        .filter(|l| l.3["sourceRole"] == "subagent")
        .map(|l| l.3["threadId"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(saved, ["unknown".to_owned()].into());
}

#[test]
fn tree_log_明确不归本运行的历史兄弟暂存到期与关闭清理都不写缺口() {
    for close in [false, true] {
        let mut m = setup();
        candidate(&mut m, "sibling", "root");
        candidate(&mut m, "unknown", "root");
        m.runs.index.child_history = serde_json::from_value(json!({
            "coverage":"verified","retainedAfter":0,
            "entries":{"sibling":{"run-old":{"rootThreadId":"root","observedAt":1}}}
        }))
        .unwrap();
        assert!(m.historical_subagent_for_run("run-a", "sibling"), "前置");
        assert!(!m.historical_subagent_for_run("run-a", "unknown"), "前置");
        let mut a = vec![];
        for thread in ["sibling", "unknown"] {
            m.buffer_child_event_log(thread, "item/completed", &item(thread, "t"), false, &mut a);
        }
        assert_eq!(m.runs.event_log.pending.len(), 2, "前置：两条都按血缘暂存");
        assert!(logs(&a).is_empty());
        a.clear();
        if close {
            m.shutdown_runs(&mut a);
        } else {
            let generation = m.runs.event_log.timer.unwrap();
            m.clock.mono_ms = PENDING_MS;
            m.sweep_event_log(generation);
            m.settle_event_log_step(&mut a);
        }
        assert!(m.runs.event_log.pending.is_empty());
        assert_eq!(m.runs.event_log.bytes, 0);
        let saved: Vec<_> = logs(&a)
            .into_iter()
            .filter(|l| l.3["sourceRole"] == "subagent")
            .map(|l| l.3["threadId"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(saved, ["unknown"], "关闭：{close}");
    }
}
impl Machine {
    /// 其他入口测试只读检查指定轮暂存与全局字节，核对结算是否释放容量。
    pub(crate) fn event_log_pending_stats(
        &self,
        thread: &str,
        turn: &str,
    ) -> (usize, usize, usize) {
        let selected: Vec<_> = self
            .runs
            .event_log
            .pending
            .iter()
            .filter(|p| p.thread == thread && p.turn == turn)
            .collect();
        (
            selected.len(),
            selected.iter().map(|p| p.bytes).sum(),
            self.runs.event_log.bytes,
        )
    }
}

fn 暂存(m: &mut Machine, turn: &str, actions: &mut Vec<Action>) {
    m.buffer_child_event_log("乙", "item/completed", &item("乙", turn), false, actions);
}

fn 核对保存(actions: &[Action], turn: &str) {
    let rows = logs(actions);
    assert!(rows.iter().all(|l| l.2 != "记录缺口"));
    let selected: Vec<_> = rows.iter().filter(|l| l.3["turnId"] == turn).collect();
    assert_eq!(selected.len(), 1);
    let row = selected[0];
    assert_eq!(row.1, "run-a");
    assert_eq!(row.2, "条目完成");
    for (key, value) in [
        ("summary", "同毫秒同正文"),
        ("itemId", "same-item"),
        ("threadId", "乙"),
        ("parentThreadId", "root"),
        ("sourceRole", "subagent"),
        ("attribution", "unconfirmed"),
    ] {
        assert_eq!(row.3[key], value);
    }
    assert_eq!(row.3["truncated"], false);
    assert!(row.3.get("coverageIssue").is_none());
}

#[test]
fn gap_wu_到期与关闭各自保存原摘要() {
    for close in [false, true] {
        let mut m = setup();
        candidate(&mut m, "乙", "root");
        let mut actions = vec![];
        暂存(&mut m, "目标", &mut actions);
        assert_eq!(m.runs.event_log.pending.len(), 1);
        m.clock.now_ms += PENDING_MS;
        m.clock.mono_ms += PENDING_MS;
        if close {
            m.shutdown_runs(&mut actions);
        } else {
            m.settle_event_log_step(&mut actions);
        }
        核对保存(&actions, "目标");
        assert!(actions.iter().any(
            |a| matches!(a, Action::Store(StoreOp::AppendLog { lines, .. })
            if lines.iter().any(|line| line.starts_with(&super::super::utc_timestamp(1000))
                && line.contains("same-item")))
        ));
        assert_eq!(m.event_log_pending_stats("乙", "目标"), (0, 0, 0));
        assert!(m.runs.event_log.timer.is_none());
    }
}

#[test]
fn gap_wu_超限直接保存新摘要且不挤掉旧暂存() {
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    let mut actions = vec![];
    暂存(&mut m, "已有", &mut actions);
    let size = m.runs.event_log.pending[0].bytes;
    for _ in 1..PER_THREAD_BYTES / size {
        暂存(&mut m, "已有", &mut actions);
    }
    assert_eq!(m.runs.event_log.bytes, PER_THREAD_BYTES / size * size);
    assert!(m.runs.event_log.bytes + size > PER_THREAD_BYTES);
    assert!(logs(&actions).is_empty());
    let old = m.runs.event_log.pending.clone();
    actions.clear();
    暂存(&mut m, "目标", &mut actions);
    核对保存(&actions, "目标");
    assert_eq!(m.runs.event_log.pending, old);
}

#[test]
fn gap_wu_同刻绑定先补写目标轮再清扫() {
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    let mut actions = vec![];
    暂存(&mut m, "先到", &mut actions);
    for id in ["目标一", "目标二"] {
        let mut params = item("乙", "目标").unwrap();
        params["item"]["id"] = json!(id);
        m.buffer_child_event_log("乙", "item/completed", &Some(params), false, &mut actions);
    }
    m.clock.mono_ms = PENDING_MS;
    bind(&mut m, "run-a", "乙", "目标", "root");
    m.flush_bound_event_log("run-a", "乙", "目标", "root", &mut actions);
    let bound = logs(&actions);
    assert_eq!(
        bound
            .iter()
            .map(|r| r.3["itemId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["目标一", "目标二"]
    );
    assert_eq!(m.runs.event_log.pending.len(), 1);
    m.settle_event_log_step(&mut actions);
    let rows = logs(&actions);
    assert_eq!(
        rows.iter()
            .map(|r| r.3["turnId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["目标", "目标", "先到"]
    );
    assert!(rows[..2].iter().all(|r| r.3.get("attribution").is_none()));
    核对保存(&actions, "先到");
}

#[test]
fn gap_wu_在途配对续期有独立上限() {
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    evidence_to(
        &mut m,
        "乙",
        EvidenceKind::V2Interacted,
        EvidenceOutcome::Pending,
        false,
        false,
    );
    let op = m.relations.operations.values_mut().next().unwrap();
    op.run_id = "run-a".into();
    op.authority_gen = 1;
    assert!(m.v2_awaits_pairing(m.relations.operations.values().next().unwrap()));
    let mut actions = vec![];
    暂存(&mut m, "目标", &mut actions);
    m.clock.mono_ms = PENDING_MS;
    m.settle_event_log_step(&mut actions);
    assert!(logs(&actions).is_empty());
    assert_eq!(m.runs.event_log.pending.len(), 1);
    m.clock.mono_ms = 119_999;
    m.facts_expire(&mut actions);
    m.settle_event_log_step(&mut actions);
    assert_eq!(m.runs.event_log.timer_deadline, Some(120_000));
    m.clock.mono_ms = 120_000;
    m.settle_event_log_step(&mut actions);
    核对保存(&actions, "目标");
    assert_eq!(m.event_log_pending_stats("乙", "目标"), (0, 0, 0));
}

#[test]
fn gap_wu_候选运行定案并淘汰前保存() {
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    evidence_to(
        &mut m,
        "乙",
        EvidenceKind::V2Interacted,
        EvidenceOutcome::Pending,
        false,
        false,
    );
    let mut actions = vec![];
    暂存(&mut m, "目标", &mut actions);
    m.run_released("run-a", crate::runs::ReleaseKind::Natural, &mut actions);
    核对保存(&actions, "目标");
    assert_eq!(m.event_log_pending_stats("乙", "目标"), (0, 0, 0));
    m.runs.notices.clear();
    m.clock.now_ms += 1;
    open(&mut m, "run-b", "root");
    m.runs.evict();
    assert!(!m.runs.runs.contains_key("run-a"));
}

#[test]
fn gap_wu_候选结束遇到已登记的绑定按已确认补写() {
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    let mut actions = vec![];
    暂存(&mut m, "目标", &mut actions);
    bind(&mut m, "run-a", "乙", "目标", "root");
    m.run_released("run-a", crate::runs::ReleaseKind::Natural, &mut actions);
    let saved: Vec<_> = logs(&actions)
        .into_iter()
        .filter(|l| l.3["turnId"] == "目标")
        .collect();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].2, "条目完成");
    assert_eq!(saved[0].3["summary"], "同毫秒同正文");
    assert!(saved[0].3.get("attribution").is_none());
    assert_eq!(m.runs.event_log.bytes, 0);
}

#[test]
fn gap_wu_证据过期后按接纳顺序取回两条摘要() {
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    evidence_to(
        &mut m,
        "乙",
        EvidenceKind::V2Interacted,
        EvidenceOutcome::Pending,
        false,
        false,
    );
    let mut actions = vec![];
    for id in ["第一条", "第二条"] {
        let params = Some(json!({"threadId":"乙","turnId":"目标","item":{
            "type":"agentMessage","id":id,"text":id
        }}));
        m.buffer_child_event_log("乙", "item/completed", &params, false, &mut actions);
        m.clock.mono_ms += PENDING_MS / 2;
    }
    m.settle_event_log_step(&mut actions);
    assert!(logs(&actions).is_empty());
    m.facts_expire(&mut actions);
    assert!(m.relations.operations.values().any(|op| op.proof_expired));
    m.clock.mono_ms += PENDING_MS;
    m.settle_event_log_step(&mut actions);
    let rows = logs(&actions);
    assert!(rows.iter().all(|l| l.2 != "记录缺口"));
    let saved: Vec<_> = rows
        .iter()
        .filter(|l| l.3["attribution"] == "unconfirmed")
        .map(|l| l.3["summary"].as_str().unwrap())
        .collect();
    assert_eq!(saved, ["第一条", "第二条"]);
}

#[test]
fn gap_wu_一份摘要分别在两个候选的结束和关闭时保存() {
    let mut m = setup();
    open(&mut m, "run-b", "root");
    candidate(&mut m, "乙", "root");
    let mut actions = vec![];
    暂存(&mut m, "目标", &mut actions);
    assert_eq!(m.runs.event_log.pending[0].candidates.len(), 2);
    m.run_released("run-a", crate::runs::ReleaseKind::Natural, &mut actions);
    核对保存(&actions, "目标");
    assert_eq!(m.runs.event_log.pending.len(), 1);
    m.clear_pending_event_log(&mut actions);
    let saved: Vec<_> = logs(&actions)
        .into_iter()
        .filter(|l| l.3["turnId"] == "目标")
        .collect();
    assert_eq!(
        saved.iter().map(|l| l.1.as_str()).collect::<Vec<_>>(),
        ["run-a", "run-b"]
    );
    assert!(saved
        .iter()
        .all(|l| l.3["attribution"] == "unconfirmed" && l.3["summary"] == "同毫秒同正文"));
    assert_eq!(m.runs.event_log.bytes, 0);
}

#[tokio::test]
async fn gap_wu_暂存原摘要提交后排空并从文件取回() {
    let temp = crate::store::tests::Temp::new();
    let (store, _) = crate::store::open(temp.config()).await.unwrap();
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    let mut actions = vec![];
    暂存(&mut m, "目标", &mut actions);
    m.clear_pending_event_log(&mut actions);
    核对保存(&actions, "目标");
    let expected = actions
        .iter()
        .find_map(|action| match action {
            Action::Store(StoreOp::AppendLog { lines, .. }) => {
                Some(format!("{}\n", lines.join("\n")))
            }
            _ => None,
        })
        .unwrap();
    for action in actions {
        if let Action::Store(op) = action {
            store.submit(op);
        }
    }
    assert!(store
        .drain(std::time::Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
    let path = temp
        .dir()
        .join(format!("runs/{}.log", m.runs.runs["run-a"].short_id));
    assert_eq!(std::fs::read_to_string(path).unwrap(), expected);
}

#[test]
fn gap_wu_同一摘要逐个判断历史候选() {
    let mut m = setup();
    open(&mut m, "run-b", "root");
    candidate(&mut m, "乙", "root");
    m.runs.index.child_history = serde_json::from_value(json!({
        "coverage":"verified","retainedAfter":0,
        "entries":{"乙":{"run-b":{"rootThreadId":"root","observedAt":1}}}
    }))
    .unwrap();
    assert!(m.historical_subagent_for_run("run-a", "乙"));
    assert!(!m.historical_subagent_for_run("run-b", "乙"));
    let mut actions = vec![];
    暂存(&mut m, "目标", &mut actions);
    assert_eq!(m.runs.event_log.pending[0].candidates.len(), 2);
    m.clear_pending_event_log(&mut actions);
    let rows = logs(&actions);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, "run-b");
    assert_eq!(rows[0].2, "条目完成");
    assert_eq!(rows[0].3["attribution"], "unconfirmed");
}

#[test]
fn gap_wu_证据实际到期并淘汰后仍保存有疑点的历史摘要() {
    let mut m = setup();
    for thread in ["乙", "对照"] {
        candidate(&mut m, thread, "root");
    }
    let old = json!({"旧运行":{"rootThreadId":"root","observedAt":1}});
    m.runs.index.child_history = serde_json::from_value(json!({
        "coverage":"verified","retainedAfter":0,"entries":{"乙":old.clone(),"对照":old}
    }))
    .unwrap();
    evidence_to(
        &mut m,
        "乙",
        EvidenceKind::V1ResumeAgent,
        EvidenceOutcome::Pending,
        false,
        false,
    );
    let key = m.relations.operations.keys().next().unwrap().clone();
    let mut actions = vec![];
    暂存(&mut m, "目标", &mut actions);
    m.buffer_child_event_log(
        "对照",
        "item/completed",
        &item("对照", "另一轮"),
        false,
        &mut actions,
    );
    assert_eq!(m.runs.event_log.pending.len(), 2);
    m.clock.mono_ms = 2;
    m.facts_expire(&mut actions);
    assert!(m.relations.operations[&key].proof_expired);
    for i in 1..MAX_OPERATIONS {
        let mut other = m.relations.operations[&key].clone();
        other.key.item_id = format!("旧条目{i}");
        other.first_seen_ms = 1;
        other.receivers.clear();
        other.proof_expired = false;
        m.relations.operations.insert(other.key.clone(), other);
    }
    m.on_relation_notification(
        &crate::claim::HeldNotification {
            method: "item/completed".into(),
            committed: false,
            params: Some(json!({"threadId":"root","turnId":"父轮","item":{
                "id":"新操作","type":"subAgentActivity","kind":"completed","agentThreadId":"其他"
            }})),
        },
        &mut actions,
    );
    assert!(!m.relations.operations.contains_key(&key));
    assert_eq!(m.relations.operations.len(), MAX_OPERATIONS);
    assert_eq!(m.runs.event_log.pending.len(), 2);
    m.clock.mono_ms = 30_000;
    m.settle_event_log_step(&mut actions);
    核对保存(&actions, "目标");
    assert!(logs(&actions).iter().all(|l| l.3["threadId"] != "对照"));
    assert!(m.runs.event_log.pending.is_empty());
}

#[test]
fn gap_wu_文件和派生等待只保护对应运行的历史候选() {
    use crate::relations::rollout_pairing::RolloutWait;
    for spawn in [false, true] {
        for owner in ["run-b", ""] {
            let mut m = setup();
            open(&mut m, "run-b", "root");
            candidate(&mut m, "乙", "root");
            m.runs.index.child_history = serde_json::from_value(json!({
                "coverage":"verified","retainedAfter":0,
                "entries":{"乙":{"旧运行":{"rootThreadId":"root","observedAt":1}}}
            }))
            .unwrap();
            evidence_to(
                &mut m,
                "乙",
                EvidenceKind::V2Interacted,
                EvidenceOutcome::Pending,
                false,
                false,
            );
            let op = m.relations.operations.values_mut().next().unwrap();
            op.run_id = owner.into();
            op.receivers.clear();
            let key = op.key.clone();
            if spawn {
                m.relations.aux.spawn_waits.insert("乙".into(), key);
            } else {
                m.relations.aux.rollout.waits.insert(
                    key,
                    RolloutWait {
                        call: None,
                        deadline_ms: 50_000,
                        wall_ms: 1000,
                        child: "乙".into(),
                        child_turns: vec![],
                        events: vec![],
                    },
                );
            }
            let mut actions = vec![];
            暂存(&mut m, "目标", &mut actions);
            assert_eq!(m.runs.event_log.pending[0].candidates.len(), 2);
            m.clock.mono_ms = 30_000;
            m.settle_event_log_step(&mut actions);
            let waiting: Vec<_> = m.runs.event_log.pending[0]
                .candidates
                .iter()
                .map(|c| c.run.as_str())
                .collect();
            let expected = if owner.is_empty() {
                vec!["run-a", "run-b"]
            } else {
                vec!["run-b"]
            };
            assert_eq!(waiting, expected);
            m.clear_pending_event_log(&mut actions);
            let rows = logs(&actions);
            assert!(rows
                .iter()
                .all(|r| r.3["attribution"] == "unconfirmed" && r.3["summary"] == "同毫秒同正文"));
            assert_eq!(
                rows.iter().map(|r| r.1.as_str()).collect::<Vec<_>>(),
                expected
            );
        }
    }
}

#[test]
fn gap_wu_树停止提交当步结算未到期候选() {
    use crate::stop::tree::{ReleaseSnapshot, TreeFinishCommit, TreeStopReport};
    let mut m = setup();
    candidate(&mut m, "乙", "root");
    let mut actions = vec![];
    暂存(&mut m, "目标", &mut actions);
    assert_eq!(m.runs.event_log.pending.len(), 1);
    let snapshot = ReleaseSnapshot {
        identity: m.manage_context("run-a").unwrap().identity.clone(),
        gen: 1,
        token: None,
        owner: None,
        parent_turn_id: None,
        parent_status: None,
        goal_status: None,
        loop_guard: false,
    };
    let children = serde_json::from_value(json!({
        "trigger":"userStop","phase":"unconfirmed","version":1,
        "root":{"thread":"root","phase":"unconfirmed","check":{"ownership":true,"turnEnded":false,"goalBraked":true,"settled":false,"verifyDone":false},"clearRequested":false,"clearConfirmed":false,"error":null},
        "stopped":[],"unconfirmed":["root"],"failed":[],"conflicts":[],"capabilityLimited":[],
        "discovery":{"missing":{},"complete":true,"version":1,"incompleteReasons":[]},"unsettledSpawns":0
    })).unwrap();
    m.commit_tree_finish(
        TreeFinishCommit {
            snapshot,
            status: crate::runs::RunStatus::Interrupted,
            interrupt_reason: None,
            report: TreeStopReport {
                tree_op: 1,
                run_id: "run-a".into(),
                generation: 1,
                round: 1,
                children,
                first_delivery_deadline_ms: 20_000,
                committed: false,
                cancelled: false,
                barrier: true,
            },
        },
        &mut actions,
    );
    assert_eq!(m.runs.runs["run-a"].state, RunState::Ended);
    核对保存(&actions, "目标");
    assert!(m.runs.event_log.pending.is_empty());
}

#[test]
fn gap_wu_接纳和计时器让同一步的绑定优先() {
    for timer in [false, true] {
        let mut m = setup();
        candidate(&mut m, "乙", "root");
        let mut actions = vec![];
        暂存(&mut m, "目标", &mut actions);
        m.clock.mono_ms = 120_000;
        if timer {
            let generation = m.runs.event_log.timer.unwrap();
            m.run_timer(RunTimer::EventLogSweep { generation }, &mut actions);
        } else {
            暂存(&mut m, "新到", &mut actions);
        }
        assert!(logs(&actions).is_empty());
        bind(&mut m, "run-a", "乙", "目标", "root");
        m.flush_bound_event_log("run-a", "乙", "目标", "root", &mut actions);
        m.settle_event_log_step(&mut actions);
        let rows = logs(&actions);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].3["turnId"], "目标");
        assert_eq!(rows[0].3["summary"], "同毫秒同正文");
        assert!(rows[0].3.get("attribution").is_none());
    }
}
