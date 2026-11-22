//! 边角缺陷块 2 的用例：消息下限（E1）与失记标志（E4、E5）。
//! 每条用例先在 A＋D 树上实跑失败，修法见交回说明。
use super::causal_pairing::{assert_no_alarm, base, op, pending, C};
use super::rollout_pairing::{awaiting, call_line, files, interacted_to, message_line, poll_for};
use super::*;
use crate::relations::rollout_pairing::{ROLLOUT_POLL_MS, ROLLOUT_WAIT_MS};
use crate::test_support::rollout as fx;

/// E1：等待下限锚错在 Interacted 到达时刻而不是调用写入时刻。
/// Interacted 晚到超过 10 秒时真实消费记录被下限挡掉且游标越过它；
/// 修法是下限降下去后从末尾窗口重读一次，核销记录。
/// （找漏洞校准轮 `bughunt_到达下限错锚丢弃真实消费记录` 移植。）
#[test]
fn gap_pe_到达下限错锚仍核销真实消费记录() {
    let (mut m, mock) = base();
    files(&m);
    call_line(&m, "跟进", "followup_task", C, "补充要求"); // 父文件行时间 = now
    m.clock.now_ms += 2_000;
    message_line(&m, C, "入站", "NEW_TASK", "补充要求"); // 子文件行时间 = now+2s，记在旧轮
                                                         // Interacted 晚到：到达时刻 now+17s，旧下限 = now+7s > 消费行时间 now+2s。
    m.clock.now_ms += 15_000;
    m.clock.mono_ms += 15_000;
    let mut all = interacted_to(&mut m, "跟进", C);
    assert!(op(&m, "跟进").v2_strict, "按文件定为严格");
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    pending(&m, 0);
    assert_eq!(op(&m, "跟进").proven_turns[C], ["旧轮".to_owned()].into());
    // 缺陷本身的断言：消费记录真的被登记了（不是靠宽松绑定收尾）。
    assert!(
        m.relations.aux.v2_deliveries.iter().any(|d| d.id == "入站"),
        "消费记录应被登记核销"
    );
    assert_no_alarm(&all);
}

/// E1 的另一侧：下限没有下移时不重读末尾窗口。
/// 子文件末尾窗口里一条比调用行早 20 秒的旧消息本来就该挡掉；定为严格后
/// 下限不变（挡掉的行时间低于下限），下一次读取带游标、不从末尾重来。
#[test]
fn gap_pe_下限未下移时不重读末尾窗口() {
    let (mut m, mock) = base();
    files(&m);
    call_line(&m, "跟进", "followup_task", C, "补充要求");
    // 比调用行早 20 秒的旧消息：挡掉它是对的，不该触发重读。
    let old = "Message Type: NEW_TASK\nTask name: /root/子\nSender: /root\nPayload:\n补充要求";
    fx::append_line(
        C,
        m.clock.now_ms - 20_000,
        "response_item",
        json!({"type":"agent_message","id":"旧消息","author":"/root","recipient":"/root/子",
            "content":[{"type":"input_text","text":old}]}),
    );
    let mut all = interacted_to(&mut m, "跟进", C);
    assert!(op(&m, "跟进").v2_strict);
    // 暂扣下一次派发，断言它带着游标而不是从末尾窗口重读。
    fx::with(|f| f.hold = true);
    all.extend(fx::poll(&mut m));
    let held = fx::with(|f| {
        f.held
            .iter()
            .filter(|r| r.thread == C)
            .map(|r| r.clone())
            .collect::<Vec<_>>()
    });
    assert!(!held.is_empty(), "跟随中的子文件应有下一次读取在途");
    assert!(
        held.iter().all(|r| r.cursor.is_some()),
        "下限未下移时不重读末尾窗口"
    );
    all.extend(fx::release_held(&mut m));
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    assert!(
        m.relations
            .aux
            .v2_deliveries
            .iter()
            .all(|d| d.id != "旧消息"),
        "旧消息没有被登记"
    );
    pending(&m, 1);
}

/// E4：失记标志按等待建立那一刻登记，不按到期那一刻查。
/// 标志在等到期之前失效时，到期判断仍判未证明——它建等待时标志确实在。
#[test]
fn gap_pe_失记标志临到期前建的等待仍判未证明() {
    let (mut m, _mock) = base();
    files(&m);
    // 预读缓存写满：之后读到的调用放不进 calls。
    let filler = crate::relations::rollout_pairing::RolloutCall {
        epoch: m.relations.conn_epoch,
        at: m.clock.mono_ms,
        wall_ms: m.clock.now_ms,
        trigger: true,
        target: Some(C.into()),
        message: None,
    };
    for n in 0..MAX_OPERATIONS {
        m.relations
            .aux
            .rollout
            .calls
            .insert((P.into(), format!("缓存{n}")), filler.clone());
    }
    call_line(&m, "留言", "send_message", C, "忙里捎话");
    // 别的操作先建等待，父文件持续被跟随：读到「留言」放不下、也没有同键等待认领，留失记标志。
    let mut all = interacted_to(&mut m, "占位", C);
    let flag = m.relations.aux.rollout.lost_calls[P];
    assert_eq!(flag.0, m.relations.conn_epoch);
    // 等到标志失效前一毫秒再建等待：建的时候标志确实在。
    m.clock.mono_ms = flag.1 - 1;
    all.extend(interacted_to(&mut m, "留言", C));
    // 等到期时标志已失效；按建立时登记仍判未证明，不按宽松规则回退绑定。
    m.clock.mono_ms += ROLLOUT_WAIT_MS;
    let mut out = vec![];
    m.drive_rollout(&mut out);
    all.extend(out);
    let operation = op(&m, "留言");
    assert_eq!(operation.outcome, EvidenceOutcome::Unproven);
    assert_eq!(
        operation.v2_fallback,
        Some(V2FallbackReason::RolloutCallMissing)
    );
    assert!(operation.proven_turns.is_empty(), "接收方的活动轮不被绑定");
    assert!(!awaiting(&m, "留言"));
    assert_no_alarm(&all);
}

/// E4 的另一侧：标志到期之后才建的等待，到期判断查不到标志，照原样宽松回退。
#[test]
fn gap_pe_失记标志过期后建的等待照原样宽松回退() {
    let (mut m, _mock) = base();
    files(&m);
    let filler = crate::relations::rollout_pairing::RolloutCall {
        epoch: m.relations.conn_epoch,
        at: m.clock.mono_ms,
        wall_ms: m.clock.now_ms,
        trigger: true,
        target: Some(C.into()),
        message: None,
    };
    for n in 0..MAX_OPERATIONS {
        m.relations
            .aux
            .rollout
            .calls
            .insert((P.into(), format!("缓存{n}")), filler.clone());
    }
    call_line(&m, "留言", "send_message", C, "忙里捎话");
    let mut all = interacted_to(&mut m, "占位", C);
    let flag = m.relations.aux.rollout.lost_calls[P];
    // 标志失效之后才建等待。
    m.clock.mono_ms = flag.1 + 1;
    m.clock.now_ms += 100;
    all.extend(interacted_to(&mut m, "留言", C));
    m.clock.mono_ms += ROLLOUT_WAIT_MS;
    let mut out = vec![];
    m.drive_rollout(&mut out);
    all.extend(out);
    let operation = op(&m, "留言");
    assert_ne!(operation.outcome, EvidenceOutcome::Unproven);
    assert_eq!(operation.proven_turns[C], ["旧轮".to_owned()].into());
    assert!(!awaiting(&m, "留言"));
    assert_no_alarm(&all);
}

/// E4 的第三侧：等待先建、标志后到。建等待时父线程还没有失记标志，
/// 之后父文件读到的调用被写满的缓存丢下才留下标志；到期时按实时判断仍判未证明。
/// 守护用例：在 A＋D 树上本来就过（旧代码到期查表），它守着「快照或实时判断任一成立」。
#[test]
fn gap_pe_等待先建标志后到到期仍判未证明() {
    let (mut m, _mock) = base();
    files(&m);
    let filler = crate::relations::rollout_pairing::RolloutCall {
        epoch: m.relations.conn_epoch,
        at: m.clock.mono_ms,
        wall_ms: m.clock.now_ms,
        trigger: true,
        target: Some(C.into()),
        message: None,
    };
    for n in 0..MAX_OPERATIONS {
        m.relations
            .aux
            .rollout
            .calls
            .insert((P.into(), format!("缓存{n}")), filler.clone());
    }
    // 先建等待：此刻还没有失记标志。
    let mut all = interacted_to(&mut m, "留言", C);
    assert!(!m.relations.aux.rollout.lost_calls.contains_key(P));
    // 之后写进父文件的「别的」被缓存丢下：标志在建等待之后才出现。
    call_line(&m, "别的", "send_message", C, "忙里捎话");
    all.extend(fx::poll(&mut m));
    assert!(
        m.relations.aux.rollout.lost_calls.contains_key(P),
        "丢下调用后父线程应留下失记标志"
    );
    // 到期时标志仍在：按实时判断判未证明，不按宽松规则回退。
    m.clock.mono_ms += ROLLOUT_WAIT_MS;
    let mut out = vec![];
    m.drive_rollout(&mut out);
    all.extend(out);
    let operation = op(&m, "留言");
    assert_eq!(operation.outcome, EvidenceOutcome::Unproven);
    assert!(operation.proven_turns.is_empty(), "接收方的活动轮不被绑定");
    assert_no_alarm(&all);
}
