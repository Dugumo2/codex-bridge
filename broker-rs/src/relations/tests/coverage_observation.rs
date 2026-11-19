//! 通过真实通知入口验证连续观察，测试不直接填入 raw 见证。
use super::*;
use crate::runs::event_log::CoverageDisposition;

const SEED: &str = "01900000-0000-7000-8000-000000000001";
const FIRST: &str = "01900000-0001-7000-8000-000000000001";
const SECOND: &str = "01900000-0002-7000-8000-000000000001";

fn notify(m: &mut Machine, method: &str, params: Value) {
    let (next, _) = crate::claim::step(
        std::mem::take(m),
        Event::Notification {
            method: method.into(),
            params: Some(params),
        },
    );
    *m = next;
}

fn start(m: &mut Machine, turn: &str) {
    notify(
        m,
        "turn/started",
        json!({"threadId":C,"turn":{"id":turn,"status":"inProgress"}}),
    );
}

fn complete(m: &mut Machine, turn: &str) {
    notify(
        m,
        "turn/completed",
        json!({"threadId":C,"turn":{"id":turn,"status":"completed","error":null}}),
    );
}

fn raw(m: &mut Machine, turn: &str, item: Value) {
    notify(
        m,
        "rawResponseItem/completed",
        json!({"threadId":C,"turnId":turn,"item":item}),
    );
}

fn reasoning(m: &mut Machine, turn: &str) {
    raw(m, turn, json!({"type":"reasoning","summary":[]}));
}

fn delivery(id: &str, body: &str) -> Value {
    json!({"type":"agent_message","id":id,"author":"/root/甲","recipient":"/root/乙",
        "content":[{"type":"input_text","text":format!("Message Type: MESSAGE\nTask name: /root/乙\nSender: /root/甲\nPayload:\n{body}")}]})
}

fn seeded() -> Machine {
    let mut m = Machine::new(false);
    start(&mut m, SEED);
    assert!(
        m.relations
            .aux
            .live_turn_starts
            .contains_key(&(C.into(), SEED.into())),
        "前置：真实开始进入原始锚点"
    );
    reasoning(&mut m, SEED);
    complete(&mut m, SEED);
    assert_eq!(
        m.coverage_observation_gen(C),
        Some(0),
        "前置：完整种子轮没有断档"
    );
    m
}

fn open(m: &mut Machine) {
    start(m, FIRST);
    assert!(
        m.coverage_open_proof(C, FIRST).is_some(),
        "前置：先验 raw 建立开放证明"
    );
    raw(m, FIRST, delivery("协作一", "内容"));
    assert_eq!(m.coverage_disposition(C, FIRST), CoverageDisposition::Wait);
}

#[test]
fn gap_b_observation_无断档真实通知连续两轮可免报() {
    let mut m = seeded();
    for turn in [FIRST, SECOND] {
        start(&mut m, turn);
        assert!(m.coverage_open_proof(C, turn).is_some());
        raw(&mut m, turn, delivery("同id跨轮", "内容"));
        notify(
            &mut m,
            "rawResponse/completed",
            json!({"threadId":C,"turnId":turn}),
        );
        assert_eq!(
            m.coverage_disposition(C, turn),
            CoverageDisposition::Wait,
            "rawResponse 不能冒充轮结束"
        );
        complete(&mut m, turn);
        assert_eq!(
            m.coverage_disposition(C, turn),
            CoverageDisposition::AllowCollaborationExclusion
        );
    }
}

#[test]
fn gap_b_observation_无开始或发现开始的raw不能授信() {
    for discovered in [false, true] {
        let mut m = Machine::new(false);
        if discovered {
            m.observe_turn(C, SEED, true, false, &mut Vec::new());
        }
        reasoning(&mut m, SEED);
        assert!(!m
            .relations
            .aux
            .live_turn_starts
            .contains_key(&(C.into(), SEED.into())));
        start(&mut m, FIRST);
        assert!(m.coverage_open_proof(C, FIRST).is_none());
        reasoning(&mut m, FIRST);
        start(&mut m, FIRST);
        assert!(
            m.coverage_open_proof(C, FIRST).is_none(),
            "本轮raw及重复started不得追认"
        );
    }
}

#[test]
fn gap_b_observation_无活动轮的未知缺口永久封锁未来() {
    for scoped in [false, true] {
        let mut m = seeded();
        let params = if scoped {
            json!({"threadId":C})
        } else {
            json!({})
        };
        notify(&mut m, "unknown/lifecycle", params);
        assert_eq!(m.coverage_observation_gen(C), None);
        start(&mut m, FIRST);
        reasoning(&mut m, FIRST);
        complete(&mut m, FIRST);
        start(&mut m, SECOND);
        assert!(m.coverage_open_proof(C, SECOND).is_none());
    }
}

#[test]
fn gap_b_observation_合法全局通知不撤销未知或坏生命周期会撤销() {
    let mut m = seeded();
    open(&mut m);
    for method in [
        "account/rateLimits/updated",
        "account/updated",
        "item/agentMessage/delta",
        "thread/name/updated",
    ] {
        notify(&mut m, method, json!({}));
        assert!(
            m.coverage_open_proof(C, FIRST).is_some(),
            "合法无关通知：{method}"
        );
    }
    notify(&mut m, "thread/status/changed", json!({"threadId":C}));
    assert!(m.coverage_open_proof(C, FIRST).is_none());
    assert_eq!(m.coverage_observation_gen(C), None);
}

#[test]
fn gap_b_observation_raw坏身份和内容均撤销但已知无关条目不撤销() {
    let invalid = [
        json!({"turnId":FIRST,"item":delivery("缺线程", "内容")}),
        json!({"threadId":C,"item":delivery("缺轮", "内容")}),
        json!({"threadId":C,"turnId":FIRST}),
        json!({"threadId":C,"turnId":FIRST,"item":{"type":"future_input"}}),
        json!({"threadId":C,"turnId":FIRST,"item":{"type":"agent_message","id":"坏头","author":"/root/甲","recipient":"/root/乙","content":[{"type":"input_text","text":"坏消息头"}]}}),
        json!({"threadId":"x".repeat(257),"turnId":FIRST,"item":delivery("坏线程", "内容")}),
    ];
    for params in invalid {
        let mut m = seeded();
        open(&mut m);
        reasoning(&mut m, FIRST);
        assert!(m.coverage_open_proof(C, FIRST).is_some());
        notify(&mut m, "rawResponseItem/completed", params);
        assert!(m.coverage_open_proof(C, FIRST).is_none());
        assert_eq!(m.coverage_observation_gen(C), None);
    }
}

#[test]
fn gap_b_observation_退订重附着和旧raw均不能恢复但封存结果保留() {
    for finished in [false, true] {
        let mut m = seeded();
        open(&mut m);
        if finished {
            complete(&mut m, FIRST);
        }
        m.detach_evidence(C);
        m.attach(C, AttachmentSource::ExplicitResume);
        assert!(m.relations.attachments[C].valid, "前置：确实重新附着");
        reasoning(&mut m, SEED);
        start(&mut m, FIRST);
        if finished {
            complete(&mut m, FIRST);
            raw(&mut m, FIRST, delivery("协作一", "内容"));
            assert_eq!(
                m.coverage_disposition(C, FIRST),
                CoverageDisposition::AllowCollaborationExclusion,
                "一致重复完成和精确重复消费保持封存"
            );
        } else {
            assert_eq!(
                m.coverage_disposition(C, FIRST),
                CoverageDisposition::OrdinaryGap
            );
        }
        start(&mut m, SECOND);
        assert!(m.coverage_open_proof(C, SECOND).is_none());
    }
}

#[test]
fn gap_b_observation_完成后新增消费或矛盾完成否决并封锁() {
    for conflict in [false, true] {
        let mut m = seeded();
        open(&mut m);
        complete(&mut m, FIRST);
        assert_eq!(
            m.coverage_disposition(C, FIRST),
            CoverageDisposition::AllowCollaborationExclusion
        );
        if conflict {
            notify(
                &mut m,
                "turn/completed",
                json!({"threadId":C,"turn":{"id":FIRST,"status":"failed"}}),
            );
        } else {
            raw(&mut m, FIRST, delivery("协作一", "冲突内容"));
        }
        assert_eq!(
            m.coverage_disposition(C, FIRST),
            CoverageDisposition::OrdinaryGap
        );
        assert_eq!(m.coverage_observation_gen(C), None);
    }
}

#[test]
fn gap_b_observation_逆序完成及完成后的raw不制造资格() {
    let mut m = Machine::new(false);
    complete(&mut m, SEED);
    start(&mut m, SEED);
    reasoning(&mut m, SEED);
    start(&mut m, FIRST);
    assert!(m.coverage_open_proof(C, FIRST).is_none());
    let mut m = Machine::new(false);
    start(&mut m, SEED);
    complete(&mut m, SEED);
    reasoning(&mut m, SEED);
    start(&mut m, FIRST);
    assert!(m.coverage_open_proof(C, FIRST).is_none());
}

#[test]
fn gap_b_observation_raw表满不淘汰封锁标记且既有干净线程继续() {
    let mut m = seeded();
    notify(&mut m, "unknown/lifecycle", json!({"threadId":"永久封锁"}));
    for i in 0..MAX_OPERATIONS - 2 {
        m.clock.mono_ms += 1;
        notify(
            &mut m,
            "thread/name/updated",
            json!({"threadId":C,"threadName":"保留观察"}),
        );
        notify(
            &mut m,
            "turn/started",
            json!({"threadId":format!("容量{i}"),"turn":{"id":SEED,"status":"inProgress"}}),
        );
    }
    assert_eq!(m.relations.aux.raw_observations.len(), MAX_OPERATIONS);
    m.clock.mono_ms += 1;
    notify(
        &mut m,
        "thread/name/updated",
        json!({"threadId":C,"threadName":"保留观察"}),
    );
    // 大量轮锚点可能触发自己的上限；只验证未淘汰线程的先验段。
    assert!(matches!(
        m.relations.aux.raw_observations.get("永久封锁"),
        Some(crate::relations::coverage_observation::RawObservation::Blocked { .. })
    ));
    notify(
        &mut m,
        "turn/started",
        json!({"threadId":"满额新线程","turn":{"id":FIRST,"status":"inProgress"}}),
    );
    assert!(!m.relations.aux.raw_observations.contains_key("满额新线程"));
    assert_eq!(m.relations.aux.raw_observations.len(), MAX_OPERATIONS);
    assert!(
        m.relations.attachments[C].valid,
        "前置：目标附着凭据没有被其他容量挤出"
    );
    assert!(
        m.relations
            .aux
            .live_turn_starts
            .contains_key(&(C.into(), SEED.into())),
        "前置：目标真实开始仍在"
    );
    assert_eq!(
        m.coverage_observation_gen(C),
        Some(0),
        "前置：目标观察段仍干净"
    );
    start(&mut m, FIRST);
    assert!(
        m.coverage_open_proof(C, FIRST).is_some(),
        "已登记干净线程不因raw表满失去先验资格"
    );
}

#[test]
fn gap_b_observation_冷恢复无raw和热恢复均不得授信() {
    for hot in [false, true] {
        let mut m = if hot { seeded() } else { Machine::new(false) };
        assert!(
            m.thread_relation(C).is_none(),
            "前置：覆盖没有血缘的订阅路径"
        );
        // 真实目标通知维持观察责任，避免成功恢复立即被空闲补偿退订遮盖。
        notify(
            &mut m,
            "thread/goal/updated",
            json!({"threadId":C,"goal":{"status":"active","objective":"持续观察"}}),
        );
        assert!(m.records[C].goal_active, "前置：确有持续观察责任");
        let context = RequestContext {
            resume_retried: false,
            stream_id: None,
            review_id: None,
            client: None,
            token: None,
            operation: Operation::ChildSubscription {
                thread: C.into(),
                op: 77,
            },
            method: "thread/resume".into(),
            params: Some(json!({"threadId":C,"excludeTurns":true})),
        };
        let mut actions = vec![Action::Request(context.clone())];
        m.child_subscriptions_after_step(&mut actions);
        assert_eq!(m.coverage_observation_gen(C), None, "发出resume前即封锁");
        m.child_register_subscription(&context, &mut actions);
        let (next, _) = crate::claim::step(
            m,
            Event::Reply {
                context,
                result: Ok(json!({"thread":{"id":C,"status":{"type":"idle"},"turns":[]}})),
            },
        );
        m = next;
        assert!(
            m.relations.attachments[C].valid,
            "前置：真实订阅回包恢复附着"
        );
        start(&mut m, FIRST);
        reasoning(&mut m, FIRST);
        complete(&mut m, FIRST);
        start(&mut m, SECOND);
        assert!(m.coverage_open_proof(C, SECOND).is_none());
    }
}

#[test]
fn gap_b_observation_开放轮开始锚点淘汰永久封锁而已完成轮保留() {
    let mut m = seeded();
    open(&mut m);
    for i in 3..=MAX_OBSERVED_TURNS + 3 {
        let turn = format!("01900000-{i:04x}-7000-8000-000000000001");
        start(&mut m, &turn);
        complete(&mut m, &turn);
    }
    assert!(
        !m.relations
            .aux
            .live_turn_starts
            .contains_key(&(C.into(), FIRST.into())),
        "前置：真实开始锚点被观察表挤出"
    );
    assert_eq!(m.coverage_observation_gen(C), None);
    assert!(m.coverage_open_proof(C, FIRST).is_none());
}

#[test]
fn gap_b_observation_无在途票据的模拟换代也永久封锁() {
    let mut m = seeded();
    assert!(
        m.relations.aux.rollout.coverage_reads.is_empty(),
        "前置：不能靠票据退休检测换代"
    );
    m.relations.conn_epoch += 1;
    start(&mut m, FIRST);
    reasoning(&mut m, FIRST);
    complete(&mut m, FIRST);
    start(&mut m, SECOND);
    assert!(m.relations.aux.raw_globally_blocked);
    assert!(m.coverage_open_proof(C, SECOND).is_none());
}

#[test]
fn gap_b_observation_种子轮已否决仍须识别畸形完成并封锁() {
    let mut m = Machine::new(false);
    start(&mut m, SEED);
    reasoning(&mut m, SEED);
    assert!(
        m.coverage_open_proof(C, SEED).is_none(),
        "前置：种子轮无先验见证而否决"
    );
    notify(
        &mut m,
        "turn/completed",
        json!({"threadId":C,"turn":{"id":SEED,"status":"futureStatus"}}),
    );
    assert_eq!(m.coverage_observation_gen(C), None);
    start(&mut m, FIRST);
    assert!(m.coverage_open_proof(C, FIRST).is_none());
}

#[test]
fn gap_b_observation_换代后仅计时器也先撤销封存证明() {
    let mut m = seeded();
    open(&mut m);
    complete(&mut m, FIRST);
    assert_eq!(
        m.coverage_disposition(C, FIRST),
        CoverageDisposition::AllowCollaborationExclusion
    );
    m.relations.conn_epoch += 1;
    let (next, _) = crate::claim::step(
        m,
        Event::Timer {
            token: Machine::relation_token(),
            kind: TimerKind::Run(crate::runs::RunTimer::EventLogSweep { generation: 999 }),
        },
    );
    assert!(next.relations.aux.raw_globally_blocked);
    assert_eq!(
        next.coverage_disposition(C, FIRST),
        CoverageDisposition::OrdinaryGap
    );
}

#[test]
fn gap_b_observation_合法活动标记保留覆盖而未知或坏状态撤销() {
    for flags in [
        json!([]),
        json!(["waitingOnApproval", "waitingOnUserInput"]),
        json!(["futureState", 7]),
        Value::Null,
    ] {
        let mut m = seeded();
        open(&mut m);
        notify(
            &mut m,
            "thread/status/changed",
            json!({"threadId":C,"status":{"type":"active","activeFlags":flags}}),
        );
        assert!(m.coverage_open_proof(C, FIRST).is_some());
    }
    for status in [json!({"type":"pendingInit"}), json!({"type":7})] {
        let mut m = seeded();
        open(&mut m);
        notify(
            &mut m,
            "thread/status/changed",
            json!({"threadId":C,"status":status}),
        );
        assert!(m.coverage_open_proof(C, FIRST).is_none());
        assert_eq!(m.coverage_observation_gen(C), None);
    }
}

#[test]
fn gap_b_review1_raw系统和开发者输入不是无关输出() {
    for role in ["system", "developer", "user", "未来角色"] {
        let mut m = seeded();
        open(&mut m);
        raw(
            &mut m,
            FIRST,
            json!({"type":"message","role":role,"content":[{"type":"input_text","text":"新增输入约束"}]}),
        );
        assert!(
            m.coverage_open_proof(C, FIRST).is_none(),
            "输入角色{role}必须否决纯协作结论"
        );
        assert_eq!(
            m.coverage_observation_gen(C),
            Some(0),
            "{role}：正常输入只影响所属轮，不是观察断档"
        );
        complete(&mut m, FIRST);
        assert_eq!(
            m.coverage_disposition(C, FIRST),
            CoverageDisposition::OrdinaryGap
        );
        start(&mut m, SECOND);
        raw(&mut m, SECOND, delivery("下一轮协作", "内容"));
        complete(&mut m, SECOND);
        assert_eq!(
            m.coverage_disposition(C, SECOND),
            CoverageDisposition::AllowCollaborationExclusion,
            "{role}：下一轮纯协作不受牵连"
        );
    }
    // 定位不到所属轮的输入无法只否决一轮，仍按断档保守处理。
    let mut m = seeded();
    open(&mut m);
    notify(
        &mut m,
        "rawResponseItem/completed",
        json!({"threadId":C,"item":{"type":"message","role":"user","content":[]}}),
    );
    assert!(m.coverage_open_proof(C, FIRST).is_none());
    assert_eq!(m.coverage_observation_gen(C), None, "缺轮号的输入封锁线程");
    // 通知上的轮不在进行中（上游轮外写历史用的内部编号、已结束的轮、没见过开始的轮）：
    // 这条输入要到之后某一轮才被消费，只否决通知上的那个编号会放过真正消费它的轮。
    for label in ["auto-compact-3", SEED, FIRST] {
        let mut m = seeded();
        raw(
            &mut m,
            label,
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":"轮外写入的输入"}]}),
        );
        assert_eq!(
            m.coverage_observation_gen(C),
            None,
            "{label}：对应不到进行中的轮，封锁线程"
        );
        start(&mut m, SECOND);
        raw(&mut m, SECOND, delivery("之后的协作", "内容"));
        complete(&mut m, SECOND);
        assert_eq!(
            m.coverage_disposition(C, SECOND),
            CoverageDisposition::OrdinaryGap,
            "{label}：之后消费了这条输入的轮不能按纯协作免报"
        );
    }
    let mut m = seeded();
    open(&mut m);
    raw(
        &mut m,
        FIRST,
        json!({"type":"message","role":"assistant","content":false}),
    );
    assert!(m.coverage_open_proof(C, FIRST).is_some());
    complete(&mut m, FIRST);
    assert_eq!(
        m.coverage_disposition(C, FIRST),
        CoverageDisposition::AllowCollaborationExclusion
    );
}
#[test]
fn gap_b_review1_attachment真实淘汰撤销开放轮并永久封锁() {
    let mut m = seeded();
    open(&mut m);
    assert!(m.relations.attachments[C].valid);
    assert_eq!(m.coverage_observation_gen(C), Some(0));
    let observed = m.relations.aux.observed.len();
    let starts = m.relations.aux.coverage_starts.len();
    let raw_threads = m.relations.aux.raw_observations.len();
    assert!(
        observed < MAX_OBSERVED_THREADS && starts < MAX_OPERATIONS && raw_threads < MAX_OPERATIONS
    );
    // 只用明确无关的真实名称通知创建附着，不登记其他线程的开始/raw/覆盖条目。
    for index in 0..MAX_ATTACHMENTS {
        m.clock.mono_ms += 1;
        notify(
            &mut m,
            "thread/name/updated",
            json!({"threadId":format!("附着容量{index}"),"threadName":"已核对名称"}),
        );
    }
    assert_eq!(
        m.relations.attachments.len(),
        MAX_ATTACHMENTS,
        "前置：确实触达附着容量上限"
    );
    assert!(
        !m.relations.attachments.contains_key(C),
        "前置：目标附着确实被生产淘汰路径删除"
    );
    assert_eq!(
        m.relations.aux.observed.len(),
        observed,
        "没有触发观察表淘汰"
    );
    assert_eq!(
        m.relations.aux.coverage_starts.len(),
        starts,
        "没有触发开始锚点淘汰"
    );
    assert_eq!(
        m.relations.aux.raw_observations.len(),
        raw_threads,
        "没有触发raw表上限"
    );
    assert!(
        m.relations
            .aux
            .live_turn_starts
            .contains_key(&(C.into(), FIRST.into())),
        "真实开始仍存在，独立锁定附着丢失分支"
    );
    assert!(
        m.coverage_open_proof(C, FIRST).is_none(),
        "开放证明必须已经转否决"
    );
    assert_eq!(
        m.coverage_disposition(C, FIRST),
        CoverageDisposition::OrdinaryGap
    );
    assert_eq!(m.coverage_observation_gen(C), None, "连续观察永久封锁");
    m.clock.mono_ms += 1;
    notify(
        &mut m,
        "thread/name/updated",
        json!({"threadId":C,"threadName":"重新附着"}),
    );
    assert!(
        m.relations.attachments[C].valid,
        "前置：之后真实通知确实恢复附着"
    );
    reasoning(&mut m, FIRST);
    complete(&mut m, FIRST);
    start(&mut m, SECOND);
    assert!(
        m.coverage_open_proof(C, SECOND).is_none(),
        "重新附着与raw不能恢复授信"
    );
    assert_eq!(m.coverage_observation_gen(C), None);
}

#[test]
fn 无关输出格式和消费记录扩展字段不撤销覆盖() {
    let mut m = seeded();
    open(&mut m);
    for item in [
        json!({"type":"commandExecution","commandActions":[7]}),
        json!({"type":"collabAgentToolCall","tool":"future","status":"future"}),
        json!({"type":"agentMessage","phase":7}),
    ] {
        notify(
            &mut m,
            "item/completed",
            json!({"threadId":C,"turnId":FIRST,"item":item}),
        );
    }
    raw(&mut m, FIRST, json!({"type":"reasoning","summary":[7]}));
    assert!(m.coverage_open_proof(C, FIRST).is_some());
    complete(&mut m, FIRST);
    let mut repeated = delivery("协作一", "内容");
    repeated["futureMetadata"] = json!({"unknown":true});
    // 精确重复的消费事实不因无关新增字段而成为迟到反证。
    raw(&mut m, FIRST, repeated);
    assert_eq!(
        m.coverage_disposition(C, FIRST),
        CoverageDisposition::AllowCollaborationExclusion
    );
}

#[test]
fn gap_fix3_重放与迟到终态不能夺回或清掉新轮() {
    for order in [
        "结束后重放",
        "替换后重放",
        "迟到完成",
        "终态先到",
        "未见过的旧开始",
    ] {
        for valid in [true, false] {
            let mut m = seeded();
            if order != "终态先到" && order != "未见过的旧开始" {
                start(&mut m, FIRST);
            }
            if matches!(order, "结束后重放" | "终态先到") {
                complete(&mut m, FIRST);
                start(&mut m, FIRST);
                if valid {
                    start(&mut m, SECOND);
                }
            } else {
                start(&mut m, SECOND);
                if order == "迟到完成" {
                    complete(&mut m, FIRST);
                } else {
                    start(&mut m, FIRST);
                }
            }
            assert!(
                m.coverage_observation_gen(C).is_some(),
                "{order}：前置，输入前仍可观察"
            );
            let turn = if valid { SECOND } else { FIRST };
            raw(&mut m, turn, json!({"type":"message","role":"user"}));
            assert_eq!(
                m.coverage_observation_gen(C).is_some(),
                valid,
                "{order}：只接受当前轮的输入"
            );
            assert!(m.coverage_open_proof(C, turn).is_none());
        }
    }
}

#[test]
fn gap_fix3_发现回查终态后旧轮输入按断档() {
    let mut m = seeded();
    open(&mut m);
    let read = crate::relations::discovery::ChildTurnRead {
        status: Some("idle".into()),
        turns: vec![(FIRST.into(), "completed".into())],
        ..Default::default()
    };
    assert!(m.convergence_apply_child_turns(R, C, &read, &mut vec![]));
    assert!(m.relations.aux.observed[C]
        .iter()
        .any(|o| o.turn == FIRST && o.completed));
    assert!(
        m.coverage_open_proof(C, FIRST).is_some(),
        "回查不能封存覆盖证明"
    );
    raw(&mut m, FIRST, json!({"type":"message","role":"developer"}));
    assert!(
        m.coverage_observation_gen(C).is_none(),
        "回查终态之后已不能归入旧轮"
    );
}

#[test]
fn gap_fix3_不可排序的新开始使旧定位失效() {
    let mut m = seeded();
    start(&mut m, FIRST);
    start(&mut m, "内部编号");
    assert!(m.coverage_observation_gen(C).is_some());
    raw(&mut m, FIRST, json!({"type":"message","role":"user"}));
    assert!(m.coverage_observation_gen(C).is_none());
}

fn gap_fix3_预处理(m: &mut Machine, method: &str, params: Value) {
    m.coverage_preprocess_notification(&crate::claim::HeldNotification {
        method: method.into(),
        params: Some(params),
        committed: false,
    });
}

#[test]
fn gap_fix3_定位容量只拒绝新线程且换代回收() {
    let mut m = Machine::new(false);
    let begin = |m: &mut Machine, thread: &str, turn: &str| {
        gap_fix3_预处理(
            m,
            "turn/started",
            json!({"threadId":thread,"turn":{"id":turn,"status":"inProgress"}}),
        )
    };
    for i in 0..MAX_OPERATIONS {
        begin(&mut m, &format!("线程{i}"), FIRST);
    }
    assert!(m.coverage_turn_running("线程0", FIRST));
    begin(&mut m, C, FIRST);
    assert!(!m.coverage_turn_running(C, FIRST), "满表拒绝新线程");
    begin(&mut m, "线程0", SECOND);
    assert!(
        m.coverage_turn_running("线程0", SECOND),
        "满表仍可替换已有轮"
    );
    m.relations.conn_epoch += 1;
    m.coverage_check_epoch();
    assert!(!m.coverage_turn_running("线程0", SECOND), "换代清掉旧定位");
    begin(&mut m, C, FIRST);
    assert!(m.coverage_turn_running(C, FIRST), "新代恢复可用容量");
}

#[test]
fn gap_fix3_单独登记的定位随生命周期清除() {
    for (method, params) in [
        ("thread/started", json!({"thread":{"id":C}})),
        (
            "thread/status/changed",
            json!({"threadId":C,"status":{"type":"notLoaded"}}),
        ),
        ("thread/archived", json!({"threadId":C})),
        ("退订", json!({})),
    ] {
        let mut m = Machine::new(false);
        m.observe_turn(C, FIRST, true, false, &mut vec![]);
        assert!(
            !m.coverage_turn_running(C, FIRST),
            "回查进行中不能充当真实开始"
        );
        start(&mut m, FIRST);
        assert!(m.coverage_turn_running(C, FIRST));
        assert!(!m.relations.aux.raw_observations.contains_key(C));
        if method == "退订" {
            m.observation_continuity_lost(C, "订阅操作切换");
        } else {
            gap_fix3_预处理(&mut m, method, params);
        }
        assert!(
            !m.coverage_turn_running(C, FIRST),
            "{method}：直接核对定位已清除"
        );
    }
}

#[test]
fn gap_fix3_同毫秒不同轮号不能反复夺回定位() {
    let other = "01900000-0001-7000-8000-000000000002";
    let mut m = seeded();
    start(&mut m, FIRST);
    start(&mut m, FIRST);
    assert!(m.coverage_turn_running(C, FIRST), "同轮重复开始幂等");
    start(&mut m, other);
    start(&mut m, FIRST);
    assert!(!m.coverage_turn_running(C, FIRST) && !m.coverage_turn_running(C, other));
    start(&mut m, SECOND);
    assert!(m.coverage_turn_running(C, SECOND));
}

#[test]
fn gap_fix3_发现回查已卸载清掉进行轮() {
    let mut m = seeded();
    open(&mut m);
    assert!(m.coverage_turn_running(C, FIRST));
    let read = crate::relations::discovery::ChildTurnRead {
        status: Some("notLoaded".into()),
        ..Default::default()
    };
    m.convergence_apply_child_turns(R, C, &read, &mut vec![]);
    assert!(!m.coverage_turn_running(C, FIRST), "回查已确认线程卸载");
}

#[test]
fn gap_fix3_同毫秒另一轮迟到完成不清当前轮() {
    let mut m = seeded();
    start(&mut m, FIRST);
    complete(&mut m, "01900000-0001-7000-8000-000000000002");
    assert!(m.coverage_turn_running(C, FIRST));
    raw(&mut m, FIRST, json!({"type":"message","role":"user"}));
    assert_eq!(m.coverage_observation_gen(C), Some(0));
}

#[test]
fn gap_fix3_旧锚点淘汰仍能定位当前输入并保留其他封存轮() {
    let mut m = seeded();
    open(&mut m);
    start(&mut m, SECOND);
    raw(&mut m, SECOND, delivery("封存", "协作"));
    complete(&mut m, SECOND);
    assert_eq!(
        m.coverage_disposition(C, SECOND),
        CoverageDisposition::AllowCollaborationExclusion
    );
    for i in 3..=MAX_OBSERVED_TURNS + 3 {
        let current = format!("01900000-{i:04x}-7000-8000-000000000001");
        start(&mut m, &current);
        if !m
            .relations
            .aux
            .coverage_starts
            .contains_key(&(C.into(), FIRST.into()))
        {
            assert!(m
                .relations
                .aux
                .coverage_starts
                .contains_key(&(C.into(), current.clone())));
            assert!(
                m.coverage_observation_gen(C).is_none(),
                "旧开放锚点淘汰已撤销后续覆盖资格"
            );
            assert_eq!(
                m.coverage_disposition(C, SECOND),
                CoverageDisposition::AllowCollaborationExclusion
            );
            raw(&mut m, &current, json!({"type":"message","role":"user"}));
            assert_eq!(
                m.coverage_disposition(C, SECOND),
                CoverageDisposition::AllowCollaborationExclusion,
                "当前输入只能否决当前轮"
            );
            return;
        }
        complete(&mut m, &current);
    }
    panic!("前置：必须实际淘汰旧开放锚点");
}
