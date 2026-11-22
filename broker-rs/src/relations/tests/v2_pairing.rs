//! 真实 v2 双通知晚于执行；只有当前代际中的唯一可核实承载轮才能消账。
use super::*;
use crate::store::StoreOp;

fn fixture() -> (Machine, SubagentMock) {
    let (mut m, mut mock) = super::lineage_safety::machine();
    mock.register(C, Some(P), "v2", false, true);
    let event = mock.thread_started(C);
    mock.inject(&mut m, event);
    let event = mock.turn_completed(C, "历史轮", "completed");
    mock.inject(&mut m, event);
    (m, mock)
}

fn begin(m: &mut Machine, mock: &mut SubagentMock) {
    mock.inject(m, Event::Notification { method: "rawResponseItem/completed".into(),
        params: Some(json!({"threadId":P,"turnId":T,"item":{"type":"function_call", "name":"followup_task","call_id":"v2操作"}})) });
}

fn interact(m: &mut Machine, mock: &mut SubagentMock) {
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, "v2操作", C);
    run(m, mock, vec![a, b]);
    m.convergence_sync_facts(R);
}

#[test]
fn v2配对无唯一轮不以任意completed轮消账() {
    for count in [0, 2] {
        let (mut m, mut mock) = fixture();
        begin(&mut m, &mut mock);
        for index in 0..count {
            let turn = format!("候选{index}");
            let event = mock.turn_started(C, &turn);
            mock.inject(&mut m, event);
            let event = mock.turn_completed(C, &turn, "completed");
            mock.inject(&mut m, event);
        }
        interact(&mut m, &mut mock);
        assert_eq!(
            m.convergences.ledgers[R].pending.len(),
            1,
            "历史完成或多候选不能证明唯一配对"
        );
        assert!(m
            .relations
            .operations
            .values()
            .all(|op| op.proven_turns.is_empty()));
        assert!(!m
            .relations
            .bindings
            .values()
            .any(|binding| binding.status == BindingStatus::Bound));
    }
}

#[test]
fn v2配对缺失开始证据不把先完成轮当作新轮() {
    let (mut m, mut mock) = fixture();
    let event = mock.turn_completed(C, "无法判明因果的轮", "completed");
    mock.inject(&mut m, event);
    interact(&mut m, &mut mock);
    assert_eq!(m.convergences.ledgers[R].pending.len(), 1);
}

#[test]
fn v2无raw回退与已选严格模式互不改写() {
    for mode in ["无开始", "晚于轮开始", "晚于Interacted", "换代丢失"] {
        let (mut m, mut mock) = fixture();
        if mode == "换代丢失" {
            begin(&mut m, &mut mock);
            m.relations.conn_epoch += 1;
        }
        let event = mock.turn_started(C, "活动轮");
        mock.inject(&mut m, event);
        if mode == "晚于轮开始" {
            begin(&mut m, &mut mock);
        }
        interact(&mut m, &mut mock);
        if mode == "晚于Interacted" {
            begin(&mut m, &mut mock);
        }
        let strict = mode == "晚于轮开始";
        assert_eq!(
            m.convergences.ledgers[R].pending.len(),
            usize::from(strict),
            "{mode}"
        );
        let event = mock.turn_completed(C, "活动轮", "completed");
        mock.inject(&mut m, event);
        assert_eq!(
            m.convergences.ledgers[R].pending.len(),
            usize::from(strict),
            "{mode}"
        );
        assert_eq!(
            m.relations
                .operations
                .values()
                .find(|op| op.key.item_id == "v2操作")
                .unwrap()
                .v2_strict,
            strict
        );
    }
}

#[test]
fn v2因果退役旧运行迟到轮不被新运行吸收() {
    let (mut m, mut mock) = super::lineage_safety::machine();
    spawn_v1(&mut m, &mut mock, C, "旧派生", "旧活动轮");
    assert!(m.binding(C, "旧活动轮").is_some());
    // 用正常退役判定回收旧绑定；首个实际开始观察仍属于旧轮，不能因回收而重新计时。
    m.claims.remove(P);
    let record = m.runs.runs.get_mut(R).unwrap();
    record.state = crate::runs::RunState::Ended;
    record.ended_at = Some(m.clock.now_ms);
    m.relations.contexts.get_mut(R).unwrap().authority = Authority::Revoked;
    m.prune_contexts();
    assert!(m.binding(C, "旧活动轮").is_none());
    m.claims
        .insert(P.into(), claim(Owner::Client(1), "新父轮", "新运行", 20));
    m.run_opened(
        crate::runs::RunOpen {
            thread_id: P.into(),
            run_id: "新运行".into(),
            conn: Some(1),
            kind: crate::claim::ReqKind::TurnStart,
            joined: false,
            input: "新任务".into(),
            continued_from: None,
        },
        &mut vec![],
    );
    m.convergence_begin(P, &mut vec![]);
    mock.inject(&mut m,Event::Notification {method:"rawResponseItem/completed".into(),params:Some(json!({
        "threadId":P,"turnId":"新父轮","item":{"type":"function_call","name":"followup_task","call_id":"新跟进"}}))});
    let event = mock.turn_started(C, "旧活动轮");
    mock.inject(&mut m, event);
    let event = mock.turn_completed(C, "旧活动轮", "completed");
    mock.inject(&mut m, event);
    let (a, b) = SubagentMock::v2_activity("interacted", P, "新父轮", "新跟进", C);
    run(&mut m, &mut mock, vec![a, b]);
    assert_eq!(m.convergences.ledgers["新运行"].pending.len(), 1);
    assert!(!m
        .relations
        .bindings
        .values()
        .any(|b| b.status == BindingStatus::Bound));
}

#[test]
fn v2配对观察缺口和not_loaded保持pending() {
    for not_loaded in [false, true] {
        let (mut m, mut mock) = fixture();
        begin(&mut m, &mut mock);
        let event = mock.turn_started(C, "真实轮");
        mock.inject(&mut m, event);
        let event = mock.turn_completed(C, "真实轮", "completed");
        mock.inject(&mut m, event);
        if not_loaded {
            mock.inject(
                &mut m,
                Event::Notification {
                    method: "thread/status/changed".into(),
                    params: Some(json!({
                "threadId":C,"status":{"type":"notLoaded"}})),
                },
            );
        } else {
            m.convergence_observation_gap(C, &mut vec![]);
        }
        interact(&mut m, &mut mock);
        assert_eq!(m.convergences.ledgers[R].pending.len(), 1);
        assert!(m
            .relations
            .operations
            .values()
            .all(|op| op.proven_turns.is_empty()));
    }
}

#[test]
fn v2配对连接换代和授权换代不能沿用旧操作() {
    for connection in [false, true] {
        let (mut m, mut mock) = fixture();
        begin(&mut m, &mut mock);
        interact(&mut m, &mut mock);
        if connection {
            m.relations.conn_epoch += 1;
        } else {
            m.relations.contexts.get_mut(R).unwrap().gen += 1;
        }
        let event = mock.turn_completed(C, "换代后轮", "completed");
        mock.inject(&mut m, event);
        m.convergence_sync_facts(R);
        assert_eq!(m.convergences.ledgers[R].pending.len(), 1);
        assert!(m
            .relations
            .operations
            .values()
            .all(|op| op.proven_turns.is_empty()));
    }
}

#[test]
fn v2配对运行中重启保留失联终态而不恢复授权或消账为成功() {
    let (mut m, mut mock) = fixture();
    begin(&mut m, &mut mock);
    interact(&mut m, &mut mock);
    assert_eq!(m.convergences.ledgers[R].pending.len(), 1);
    let record = m.runs.runs[R].clone();
    let mut restored = Machine::default();
    mock.inject(
        &mut restored,
        Event::Restore {
            index: m.runs.index.clone(),
            runs: vec![record],
            inbox: Default::default(),
        },
    );
    assert_eq!(restored.runs.runs[R].state, crate::runs::RunState::Lost);
    assert!(
        restored.runs.runs[R]
            .subagent_join
            .as_ref()
            .unwrap()
            .automatic_control_revoked
    );
    assert!(restored.convergence_diagnostics(R).is_none());
    assert!(restored.convergences.runs.is_empty());
    let event = mock.turn_completed(C, "重启后迟到轮", "completed");
    mock.inject(&mut restored, event);
    interact(&mut restored, &mut mock);
    assert!(!restored
        .relations
        .bindings
        .values()
        .any(|binding| binding.status == BindingStatus::Bound));
    assert_eq!(restored.runs.runs[R].state, crate::runs::RunState::Lost);
}

fn log_lines_with<'a>(actions: &'a [Action], text: &str) -> Vec<&'a String> {
    actions
        .iter()
        .flat_map(|a| match a {
            Action::Store(StoreOp::AppendLog { lines, .. }) => lines.iter().collect::<Vec<_>>(),
            _ => vec![],
        })
        .filter(|line| line.contains(text))
        .collect()
}

#[test]
fn v2子代理向上发消息不记归属冲突而向下跟进非直接子仍拒绝() {
    const GRANDCHILD: &str = "孙线程";
    let (mut m, mut mock) = fixture();
    begin(&mut m, &mut mock);
    interact(&mut m, &mut mock);
    let event = mock.turn_started(C, "子轮");
    mock.inject(&mut m, event);
    assert!(m.binding(C, "子轮").is_some(), "前置：子轮已受本运行管理");
    mock.register(GRANDCHILD, Some(C), "v2", false, true);
    let event = mock.thread_started(GRANDCHILD);
    mock.inject(&mut m, event);
    let (a, b) = SubagentMock::v2_activity("started", C, "子轮", "派生孙", GRANDCHILD);
    let event = mock.turn_started(GRANDCHILD, "孙轮");
    run(&mut m, &mut mock, vec![a, b, event]);
    assert!(
        m.binding(GRANDCHILD, "孙轮").is_some(),
        "前置：孙轮已受本运行管理"
    );

    // 子向父、孙隔代向根：都是正常向上消息。
    for (sender, turn, item) in [(C, "子轮", "子向上"), (GRANDCHILD, "孙轮", "孙向上")] {
        let (a, b) = SubagentMock::v2_activity("interacted", sender, turn, item, P);
        let actions = run(&mut m, &mut mock, vec![a, b]);
        m.convergence_sync_facts(R);
        let op = m
            .relations
            .operations
            .values()
            .find(|op| op.key.item_id == item)
            .expect("前置：向上消息确实进入跟进判定");
        assert!(op.rejected_receivers.contains_key(P), "{item} 不授予管理权");
        assert!(log_lines_with(&actions, "归属冲突").is_empty(), "{item}");
        assert!(
            log_lines_with(&actions, "discoveryIncomplete").is_empty(),
            "{item}"
        );
        assert!(!m.manage_context(R).unwrap().members.contains_key(P));
    }
    assert!(
        m.convergences.ledgers[R].pending.is_empty(),
        "向上消息不留待定操作"
    );

    // 父线程直接跟进孙代理不是向上，C 的直接父校验仍要拒绝并记冲突。
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, "越级跟进", GRANDCHILD);
    let actions = run(&mut m, &mut mock, vec![a, b]);
    let conflicts = log_lines_with(&actions, "归属冲突");
    assert_eq!(conflicts.len(), 1);
    assert!(conflicts[0].contains("未核实为本父线程的子代理"));
}

#[test]
fn v2兄弟与隔房子代理互发不记归属冲突且不授管理权() {
    const SIBLING: &str = "兄弟线程";
    const NEPHEW: &str = "侄线程";
    let (mut m, mut mock) = fixture();
    begin(&mut m, &mut mock);
    interact(&mut m, &mut mock);
    let event = mock.turn_started(C, "子轮");
    mock.inject(&mut m, event);
    for (thread, parent, item, turn) in [
        (SIBLING, P, "派生兄弟", "兄弟轮"),
        (NEPHEW, SIBLING, "派生侄", "侄轮"),
    ] {
        mock.register(thread, Some(parent), "v2", false, true);
        let event = mock.thread_started(thread);
        mock.inject(&mut m, event);
        let parent_turn = if parent == P { T } else { "兄弟轮" };
        let (a, b) = SubagentMock::v2_activity("started", parent, parent_turn, item, thread);
        let event = mock.turn_started(thread, turn);
        run(&mut m, &mut mock, vec![a, b, event]);
    }
    for (item, target) in [("互发兄弟", SIBLING), ("互发侄", NEPHEW)] {
        let (a, b) = SubagentMock::v2_activity("interacted", C, "子轮", item, target);
        let actions = run(&mut m, &mut mock, vec![a, b]);
        m.convergence_sync_facts(R);
        let op = m
            .relations
            .operations
            .values()
            .find(|op| op.key.item_id == item)
            .unwrap();
        assert!(
            op.rejected_receivers[target].reason().contains("平级"),
            "{item}"
        );
        assert!(log_lines_with(&actions, "归属冲突").is_empty(), "{item}");
        assert!(
            log_lines_with(&actions, "discoveryIncomplete").is_empty(),
            "{item}"
        );
    }
    assert!(m.convergences.ledgers[R].pending.is_empty());

    // 关系不明的线程不是平级，仍要记冲突。
    let (a, b) = SubagentMock::v2_activity("interacted", C, "子轮", "互发未知", "未知线程");
    let actions = run(&mut m, &mut mock, vec![a, b]);
    assert_eq!(log_lines_with(&actions, "归属冲突").len(), 1);

    // 隔代下级也不是平级：跨过直接子代理跟进仍要拒绝并记冲突。
    const GRAND_NEPHEW: &str = "侄孙线程";
    mock.register(GRAND_NEPHEW, Some(NEPHEW), "v2", false, true);
    let event = mock.thread_started(GRAND_NEPHEW);
    mock.inject(&mut m, event);
    let (a, b) = SubagentMock::v2_activity("started", NEPHEW, "侄轮", "派生侄孙", GRAND_NEPHEW);
    let event = mock.turn_started(GRAND_NEPHEW, "侄孙轮");
    run(&mut m, &mut mock, vec![a, b, event]);
    let (a, b) = SubagentMock::v2_activity(
        "interacted",
        SIBLING,
        "兄弟轮",
        "越级跟进侄孙",
        GRAND_NEPHEW,
    );
    let actions = run(&mut m, &mut mock, vec![a, b]);
    let conflicts = log_lines_with(&actions, "归属冲突");
    assert_eq!(conflicts.len(), 1);
    assert!(conflicts[0].contains("未核实为本父线程的子代理"));
}

/// 两表都满且留言身份失记：真实 followup_task（raw 调用＋Interacted）在证据期限内
/// 计入 pending 给真实子轮留出开始时间；期限到了移出 pending 并清掉占位，运行可进入
/// 汇总而不是等到收敛总期限。不误绑保持：不产生绑定。对照：留言身份表没满时，
/// 同样的跟进仍按 StartMissing 宽松配对。
#[test]
fn gap_wu_身份失记的跟进按证据期限结算() {
    for full in [true, false] {
        let (mut m, mut mock) = super::lineage_safety::machine();
        // 本运行派生的非历史子线程：正文归属只看本运行候选，不依赖块 1。
        spawn_v1(&mut m, &mut mock, C, "派生子线程", "首轮");
        for index in 0..MAX_OPERATIONS {
            m.relations.aux.followup_starts.insert(
                EvidenceKey {
                    parent_thread_id: P.into(),
                    parent_turn_id: T.into(),
                    item_id: format!("占位开始{index}"),
                },
                crate::relations::v2_pairing::FollowupStart {
                    epoch: m.relations.conn_epoch,
                    seq: index as u64,
                    at: m.clock.mono_ms,
                    source: EvidenceSource::Raw,
                    wall_ms: m.clock.now_ms,
                    run: R.into(),
                    generation: 1,
                    target: None,
                    message: None,
                    trigger: true,
                },
            );
        }
        if full {
            let epoch = m.relations.conn_epoch;
            for index in 0..MAX_OPERATIONS {
                m.relations.aux.raw_queue_receipts.insert(
                    EvidenceKey {
                        parent_thread_id: P.into(),
                        parent_turn_id: T.into(),
                        item_id: format!("占位留言{index}"),
                    },
                    (epoch, m.clock.mono_ms + EVIDENCE_TTL_MS),
                );
            }
            // 再放一条放不下身份的留言，让本批证据进入「身份失记」窗口。
            mock.inject(
                &mut m,
                Event::Notification {
                    method: "rawResponseItem/completed".into(),
                    params: Some(json!({"threadId":P,"turnId":T,
                        "item":{"type":"function_call","name":"send_message","call_id":"溢出留言"}})),
                },
            );
            assert!(
                m.relations.aux.raw_queue_receipts_full.is_some(),
                "前置：留言身份表也放不下"
            );
        }
        // 真实跟进：raw 调用到达时开始表放不下，Interacted 到来时身份已失记。
        mock.inject(
            &mut m,
            Event::Notification {
                method: "rawResponseItem/completed".into(),
                params: Some(json!({"threadId":P,"turnId":T,
                    "item":{"type":"function_call","name":"followup_task","call_id":"失记跟进"}})),
            },
        );
        let (a, b) = SubagentMock::v2_activity("interacted", P, T, "失记跟进", C);
        run(&mut m, &mut mock, vec![a, b]);
        m.convergence_sync_facts(R);
        let key = EvidenceKey {
            parent_thread_id: P.into(),
            parent_turn_id: T.into(),
            item_id: "失记跟进".into(),
        };
        let op = m
            .relations
            .operations
            .get(&key)
            .expect("跟进登记了证据操作");
        if !full {
            assert_eq!(op.v2_fallback, Some(V2FallbackReason::StartMissing));
            assert_ne!(
                op.outcome,
                EvidenceOutcome::Unproven,
                "留言身份表没满时仍按 StartMissing 宽松配对"
            );
            continue;
        }
        assert_eq!(op.outcome, EvidenceOutcome::Unproven);
        assert!(
            m.convergences.ledgers[R].pending.contains(&key),
            "期限内失记跟进仍计入 pending，给真实子轮留开始时间"
        );
        // 子线程开新轮并输出：轮次不误绑，正文按归属未确认暂存。
        let event = mock.turn_started(C, "子轮");
        mock.inject(&mut m, event);
        assert!(
            !m.binding(C, "子轮")
                .is_some_and(|b| b.status == BindingStatus::Bound),
            "失记跟进不授予管理权"
        );
        m.buffer_child_event_log(
            C,
            "item/completed",
            &Some(json!({"threadId":C,"turnId":"子轮",
                "item":{"id":"子轮输出","type":"agentMessage","text":"失记跟进的实际正文"}})),
            false,
            &mut vec![],
        );
        // 子轮结束且到期后：移出 pending、清占位，运行可进入汇总。
        let event = mock.turn_completed(C, "子轮", "completed");
        mock.inject(&mut m, event);
        m.convergence_sync_facts(R);
        assert!(
            m.convergences.ledgers[R].pending.contains(&key),
            "30 秒内操作仍在 pending"
        );
        let mut settled = advance(&mut m, &mut mock, EVIDENCE_TTL_MS);
        assert!(
            !m.convergences.ledgers[R].pending.contains(&key),
            "证据期限到了失记跟进要移出 pending，不能挂到收敛总期限"
        );
        assert!(
            m.convergences.ledgers[R]
                .placeholder_operations
                .values()
                .all(|reasons| !reasons.contains(&key)),
            "占位也随结算清掉"
        );
        assert_eq!(
            m.convergence_facts(R).unwrap().pending_starts,
            0,
            "结算后运行可进入汇总"
        );
        // 到期结算后暂存正文按归属未确认写回，仍可取回。
        m.settle_event_log_step(&mut settled);
        assert!(
            log_lines_with(&settled, "unconfirmed")
                .iter()
                .any(|line| line.contains("失记跟进的实际正文")),
            "正文按归属未确认保存并可取回"
        );
    }
}

/// 接缝：历史子线程（旧运行派生、本次只补记到 child_history）的失记跟进同样
/// 按证据期限结算：计入 pending 给子轮留开始时间、到期移出并清占位、
/// 正文按归属未确认写回可取回。A 之后结算不看历史分类，口径与本运行派生一致。
#[test]
fn gap_wu_历史子线程的失记跟进按证据期限结算() {
    let (mut m, mut mock) = super::lineage_safety::machine();
    // 历史子线程：登记元数据并记入 child_history，不经本运行的 v1 派生。
    mock.register(C, Some(P), "v1", false, true);
    super::lineage_safety::history(&mut m, C);
    for index in 0..MAX_OPERATIONS {
        m.relations.aux.followup_starts.insert(
            EvidenceKey {
                parent_thread_id: P.into(),
                parent_turn_id: T.into(),
                item_id: format!("占位开始{index}"),
            },
            crate::relations::v2_pairing::FollowupStart {
                epoch: m.relations.conn_epoch,
                seq: index as u64,
                at: m.clock.mono_ms,
                source: EvidenceSource::Raw,
                wall_ms: m.clock.now_ms,
                run: R.into(),
                generation: 1,
                target: None,
                message: None,
                trigger: true,
            },
        );
    }
    let epoch = m.relations.conn_epoch;
    for index in 0..MAX_OPERATIONS {
        m.relations.aux.raw_queue_receipts.insert(
            EvidenceKey {
                parent_thread_id: P.into(),
                parent_turn_id: T.into(),
                item_id: format!("占位留言{index}"),
            },
            (epoch, m.clock.mono_ms + EVIDENCE_TTL_MS),
        );
    }
    mock.inject(
        &mut m,
        Event::Notification {
            method: "rawResponseItem/completed".into(),
            params: Some(json!({"threadId":P,"turnId":T,
                "item":{"type":"function_call","name":"send_message","call_id":"溢出留言"}})),
        },
    );
    mock.inject(
        &mut m,
        Event::Notification {
            method: "rawResponseItem/completed".into(),
            params: Some(json!({"threadId":P,"turnId":T,
                "item":{"type":"function_call","name":"followup_task","call_id":"失记跟进"}})),
        },
    );
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, "失记跟进", C);
    run(&mut m, &mut mock, vec![a, b]);
    m.convergence_sync_facts(R);
    let key = EvidenceKey {
        parent_thread_id: P.into(),
        parent_turn_id: T.into(),
        item_id: "失记跟进".into(),
    };
    let op = m
        .relations
        .operations
        .get(&key)
        .expect("跟进登记了证据操作");
    assert_eq!(op.outcome, EvidenceOutcome::Unproven);
    assert!(
        m.convergences.ledgers[R].pending.contains(&key),
        "期限内失记跟进仍计入 pending"
    );
    let event = mock.turn_started(C, "子轮");
    mock.inject(&mut m, event);
    assert!(
        !m.binding(C, "子轮")
            .is_some_and(|b| b.status == BindingStatus::Bound),
        "失记跟进不授予管理权"
    );
    m.buffer_child_event_log(
        C,
        "item/completed",
        &Some(json!({"threadId":C,"turnId":"子轮",
            "item":{"id":"子轮输出","type":"agentMessage","text":"历史子线程的失记跟进正文"}})),
        false,
        &mut vec![],
    );
    let event = mock.turn_completed(C, "子轮", "completed");
    mock.inject(&mut m, event);
    m.convergence_sync_facts(R);
    assert!(m.convergences.ledgers[R].pending.contains(&key));
    let mut settled = advance(&mut m, &mut mock, EVIDENCE_TTL_MS);
    assert!(
        !m.convergences.ledgers[R].pending.contains(&key),
        "证据期限到了移出 pending"
    );
    assert!(m.convergences.ledgers[R]
        .placeholder_operations
        .values()
        .all(|reasons| !reasons.contains(&key)));
    m.settle_event_log_step(&mut settled);
    assert!(
        log_lines_with(&settled, "unconfirmed")
            .iter()
            .any(|line| line.contains("历史子线程的失记跟进正文")),
        "历史子线程的正文同样按归属未确认写回可取回"
    );
}

/// P1-2 正文落盘再读回：结算写回的归属未确认行提交真实存储后，
/// 从磁盘文件读回仍是同一行（不止停在写日志的动作上）。
#[tokio::test]
async fn gap_wu_失记跟进的正文落盘后可读回() {
    let temp = crate::store::tests::Temp::new();
    let (store, _) = crate::store::open(temp.config()).await.unwrap();
    let (mut m, mut mock) = super::lineage_safety::machine();
    spawn_v1(&mut m, &mut mock, C, "派生子线程", "首轮");
    for index in 0..MAX_OPERATIONS {
        m.relations.aux.followup_starts.insert(
            EvidenceKey {
                parent_thread_id: P.into(),
                parent_turn_id: T.into(),
                item_id: format!("占位开始{index}"),
            },
            crate::relations::v2_pairing::FollowupStart {
                epoch: m.relations.conn_epoch,
                seq: index as u64,
                at: m.clock.mono_ms,
                source: EvidenceSource::Raw,
                wall_ms: m.clock.now_ms,
                run: R.into(),
                generation: 1,
                target: None,
                message: None,
                trigger: true,
            },
        );
    }
    let epoch = m.relations.conn_epoch;
    for index in 0..MAX_OPERATIONS {
        m.relations.aux.raw_queue_receipts.insert(
            EvidenceKey {
                parent_thread_id: P.into(),
                parent_turn_id: T.into(),
                item_id: format!("占位留言{index}"),
            },
            (epoch, m.clock.mono_ms + EVIDENCE_TTL_MS),
        );
    }
    mock.inject(
        &mut m,
        Event::Notification {
            method: "rawResponseItem/completed".into(),
            params: Some(json!({"threadId":P,"turnId":T,
                "item":{"type":"function_call","name":"send_message","call_id":"溢出留言"}})),
        },
    );
    mock.inject(
        &mut m,
        Event::Notification {
            method: "rawResponseItem/completed".into(),
            params: Some(json!({"threadId":P,"turnId":T,
                "item":{"type":"function_call","name":"followup_task","call_id":"失记跟进"}})),
        },
    );
    let (a, b) = SubagentMock::v2_activity("interacted", P, T, "失记跟进", C);
    run(&mut m, &mut mock, vec![a, b]);
    m.convergence_sync_facts(R);
    let key = EvidenceKey {
        parent_thread_id: P.into(),
        parent_turn_id: T.into(),
        item_id: "失记跟进".into(),
    };
    assert_eq!(
        m.relations.operations[&key].outcome,
        EvidenceOutcome::Unproven,
        "两表都满，跟进身份失记"
    );
    let event = mock.turn_started(C, "子轮");
    mock.inject(&mut m, event);
    m.buffer_child_event_log(
        C,
        "item/completed",
        &Some(json!({"threadId":C,"turnId":"子轮",
            "item":{"id":"子轮输出","type":"agentMessage","text":"失记跟进的实际正文"}})),
        false,
        &mut vec![],
    );
    let event = mock.turn_completed(C, "子轮", "completed");
    mock.inject(&mut m, event);
    m.convergence_sync_facts(R);
    let mut settled = advance(&mut m, &mut mock, EVIDENCE_TTL_MS);
    m.settle_event_log_step(&mut settled);
    let expected = settled
        .iter()
        .flat_map(|a| match a {
            Action::Store(StoreOp::AppendLog { lines, .. }) => lines.clone(),
            _ => vec![],
        })
        .collect::<Vec<_>>();
    assert!(
        expected
            .iter()
            .any(|line| line.contains("失记跟进的实际正文") && line.contains("unconfirmed")),
        "结算写回了归属未确认的正文行"
    );
    for action in &settled {
        if let Action::Store(op) = action {
            store.submit(op.clone());
        }
    }
    assert!(store
        .drain(std::time::Duration::from_secs(2))
        .await
        .abandoned
        .is_empty());
    let path = temp
        .dir()
        .join(format!("runs/{}.log", m.runs.runs[R].short_id));
    let on_disk = std::fs::read_to_string(path).unwrap();
    assert_eq!(
        on_disk,
        format!("{}\n", expected.join("\n")),
        "落盘内容与写出行一致"
    );
    assert!(
        on_disk.contains("失记跟进的实际正文") && on_disk.contains("unconfirmed"),
        "从磁盘读回正文仍在且归属未确认"
    );
}

#[test]
fn v2乱序消费暂存有界并按ttl清理() {
    let (mut m, _) = fixture();
    for index in 0..=MAX_OPERATIONS {
        let item = json!({"type":"agent_message","id":format!("input-{index}"),"author":"/root","recipient":format!("/root/{C}"),
            "content":[{"type":"input_text","text":format!("Message Type: NEW_TASK\nTask name: /root/{C}\nSender: /root\nPayload:\n输入{index}")}]});
        m.v2_observe_raw(C, "先到轮", &item, &mut vec![]);
    }
    assert_eq!(m.relations.aux.v2_deliveries.len(), MAX_OPERATIONS);
    m.clock.mono_ms += EVIDENCE_TTL_MS;
    m.prune_v2_deliveries();
    assert!(m.relations.aux.v2_deliveries.is_empty());
}

#[test]
fn v2配对索引与原表始终一致且挤出后同号可再登记() {
    let (mut m, mut mock) = fixture();
    let delivery = |index: usize| {
        json!({"type":"agent_message","id":format!("input-{index}"),"author":"/root","recipient":format!("/root/{C}"),
            "content":[{"type":"input_text","text":format!("Message Type: NEW_TASK\nTask name: /root/{C}\nSender: /root\nPayload:\n输入{}", index % 3)}]})
    };
    begin(&mut m, &mut mock);
    for index in 0..=MAX_OPERATIONS {
        m.v2_observe_raw(C, "先到轮", &delivery(index), &mut vec![]);
        assert!(m.v2_index_consistent(), "第 {index} 条后索引失配");
    }
    // 最早一条已被挤出：同号再到是新事实，不能被旧索引当成重复丢弃。
    assert!(!m
        .relations
        .aux
        .v2_deliveries
        .iter()
        .any(|d| d.id == "input-0"));
    m.v2_observe_raw(C, "先到轮", &delivery(0), &mut vec![]);
    assert!(m
        .relations
        .aux
        .v2_deliveries
        .iter()
        .any(|d| d.id == "input-0"));
    // 重复到达的同号输入仍只登记一次。
    let before = m.relations.aux.v2_deliveries.len();
    m.v2_observe_raw(C, "先到轮", &delivery(0), &mut vec![]);
    assert_eq!(m.relations.aux.v2_deliveries.len(), before);
    assert!(m.v2_index_consistent());
    m.clock.mono_ms += EVIDENCE_TTL_MS;
    m.prune_v2_deliveries();
    m.prune_followup_starts();
    assert!(m.relations.aux.v2_deliveries.is_empty());
    assert!(m.relations.aux.followup_starts.is_empty());
    assert!(m.v2_index_consistent());
    assert_eq!(m.relations.aux.v2_index, Default::default());
}
