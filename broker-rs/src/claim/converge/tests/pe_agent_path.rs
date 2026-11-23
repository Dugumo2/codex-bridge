//! 边角缺陷块 3 收口（E6）：接收方代理路径超过 broker 登记上限（256 字节）的跟进。
//! 超长路径登记不进 `v2_receiver_paths`，定为严格后永远配不出候选、死等到收敛期限；
//! 修法是路径超长的跟进不定为严格，按宽松规则结算（留言不受影响）。
//! 用例先在套好五个提交的树上实跑失败，修法见收口汇报。
use super::causal_pairing::{assert_no_alarm, base, op, pending, raw, C};
use super::rollout_pairing::{call_line, files, message_line, poll_for};
use super::*;
use crate::relations::rollout_pairing::ROLLOUT_POLL_MS;
use crate::test_support::rollout as fx;

/// Interacted 条目带自定 `agentPath`（上游只限字符集不限长度；用超长 ASCII 名）。
fn interacted_path(m: &mut Machine, id: &str, child: &str, path: &str) -> Vec<Action> {
    let item = json!({"type": "subAgentActivity", "id": id, "kind": "interacted",
        "agentThreadId": child, "agentPath": path});
    let mut out = notify(
        m,
        "item/started",
        json!({"threadId": P, "turnId": T, "item": item.clone()}),
    );
    out.extend(notify(
        m,
        "item/completed",
        json!({"threadId": P, "turnId": T, "item": item}),
    ));
    out
}

/// 306 字节的接收方路径（上限 256）。
fn 超长路径() -> String {
    format!("/root/{}", "a".repeat(300))
}

/// E6 文件一侧：父线程没送过 raw，跟进由父文件里的 followup_task 调用定位。
/// 路径超长时定为严格永远配不出候选、死等到收敛期限；
/// 修法是直接按宽松规则结算，不再等。
#[test]
fn gap_pe_超长接收方路径的文件跟进按宽松配对() {
    let (mut m, mock) = base();
    files(&m);
    call_line(&m, "跟进", "followup_task", C, "补充要求");
    message_line(&m, C, "入站", "NEW_TASK", "补充要求");
    let mut all = interacted_path(&mut m, "跟进", C, &超长路径());
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    let operation = op(&m, "跟进");
    assert!(!operation.v2_strict, "超长路径不登记，不该定为严格");
    assert_eq!(
        operation.v2_fallback,
        Some(V2FallbackReason::ReceiverPathTooLong)
    );
    pending(&m, 0);
    assert_no_alarm(&all);
}

/// E6 raw 一侧：父线程有 raw 开始记录的同一场景。
/// 路径超长时开始记录在也救不了配对——接收方路径登记不了，
/// 建操作时就定为宽松，不走死等。
#[test]
fn gap_pe_超长接收方路径的raw跟进按宽松配对() {
    let (mut m, mock) = base();
    files(&m);
    raw(&mut m, "跟进", "补充要求");
    message_line(&m, C, "入站", "NEW_TASK", "补充要求");
    let mut all = interacted_path(&mut m, "跟进", C, &超长路径());
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    let operation = op(&m, "跟进");
    assert!(!operation.v2_strict, "超长路径不登记，不该定为严格");
    assert_eq!(
        operation.v2_fallback,
        Some(V2FallbackReason::ReceiverPathTooLong)
    );
    pending(&m, 0);
    assert_no_alarm(&all);
}

/// 守护另一侧：路径恰好到上限（256 字节）仍登记，照旧定为严格、配对成功。
#[test]
fn gap_pe_接收方路径恰到上限仍按严格配对() {
    let (mut m, mock) = base();
    files(&m);
    call_line(&m, "跟进", "followup_task", C, "补充要求");
    let path = format!("/root/{}", "b".repeat(250)); // 恰好 256 字节
    assert_eq!(path.len(), 256);
    // 消费记录的 recipient 必须等于登记的接收方路径：这里写成同一条 256 字节路径。
    let text =
        format!("Message Type: NEW_TASK\nTask name: {path}\nSender: /root\nPayload:\n补充要求");
    fx::append_line(
        C,
        m.clock.now_ms,
        "response_item",
        json!({"type":"agent_message","id":"入站","author":"/root","recipient":path,
            "content":[{"type":"input_text","text":text}]}),
    );
    let mut all = interacted_path(&mut m, "跟进", C, &path);
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    let operation = op(&m, "跟进");
    assert!(operation.v2_strict, "恰到上限仍定为严格");
    pending(&m, 0);
    assert_eq!(operation.proven_turns[C], ["旧轮".to_owned()].into());
    assert_no_alarm(&all);
}

/// 守护另一侧：留言（send_message）只入队、不需要接收方路径，照旧定为严格；
/// 超长路径对它不加分支、不占账本。
#[test]
fn gap_pe_超长接收方路径的留言仍只入队() {
    let (mut m, mock) = base();
    files(&m);
    call_line(&m, "留言", "send_message", C, "忙里捎话");
    let mut all = interacted_path(&mut m, "留言", C, &超长路径());
    all.extend(poll_for(&mut m, &mock, ROLLOUT_POLL_MS));
    let operation = op(&m, "留言");
    assert!(operation.v2_strict, "留言照旧定为严格");
    assert!(operation.v2_queue_only, "留言只入队");
    pending(&m, 0);
    assert_no_alarm(&all);
}
