//! 边角缺陷块 2 的用例：消息下限（E1）与失记标志（E4、E5）。
//! 每条用例先在 A＋D 树上实跑失败，修法见交回说明。
use super::causal_pairing::{assert_no_alarm, base, op, pending, C};
use super::rollout_pairing::{call_line, files, interacted_to, message_line, poll_for};
use super::*;
use crate::relations::rollout_pairing::ROLLOUT_POLL_MS;
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
