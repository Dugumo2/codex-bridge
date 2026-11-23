//! P2-5、P2-7 边角：文件表淘汰时保留冷却截止；断档退休在途路径请求。
use super::causal_pairing::{base, op, C};
use super::rollout_pairing::{call_line, child_edge, files, interacted_to};
use super::*;
use crate::claim::Operation;
use crate::relations::rollout_pairing::{
    RolloutFile, RolloutPathState, MAX_ROLLOUT_FILES, ROLLOUT_RETRY_MS, ROLLOUT_WAIT_MS,
};
use crate::test_support::rollout as fx;

/// 动作里对某线程的 thread/read 路径请求数。
fn path_requests(actions: &[Action], thread: &str) -> usize {
    actions
        .iter()
        .filter(|a| {
            matches!(a, Action::Request(c) if matches!(&c.operation,
                Operation::RolloutPath { thread: t, .. } if t.as_str() == thread))
        })
        .count()
}

/// 动作里某线程路径请求的代号。
fn path_generation(actions: &[Action], thread: &str) -> Option<u64> {
    actions.iter().find_map(|a| match a {
        Action::Request(c) => match &c.operation {
            Operation::RolloutPath {
                thread: t,
                generation,
            } if t.as_str() == thread => Some(*generation),
            _ => None,
        },
        _ => None,
    })
}

/// 建等待并截下在途路径请求（不经 service，回包由用例自己喂）。
fn pending_path(m: &mut Machine, id: &str, child: &str) -> Vec<Action> {
    let (start, _done) = SubagentMock::v2_activity("interacted", P, T, id, child);
    let (next, actions) = step(std::mem::take(m), start);
    *m = next;
    actions
}

/// 「子」的路径请求失败：进入冷却（截止 = now + ROLLOUT_RETRY_MS）。
fn fail_child_path(m: &mut Machine) {
    fx::with(|f| {
        f.replies
            .insert(C.into(), Err(json!({"message": "取不到路径"})))
    });
    call_line(m, "跟进", "followup_task", C, "任务");
    interacted_to(m, "跟进", C);
}

/// 排在「子」后的冷却文件填满文件表（淘汰只能落在「子」上）。
fn fill_cooling(m: &mut Machine, next_at: u64) {
    let epoch = m.relations.conn_epoch;
    for i in 0..MAX_ROLLOUT_FILES - 2 {
        m.relations.aux.rollout.files.insert(
            format!("线程{i}"),
            RolloutFile {
                epoch,
                path: RolloutPathState::Failed(V2FallbackReason::RolloutUnreadable),
                cursor: None,
                reading: None,
                reads: 0,
                next_at,
                floor_dropped: None,
            },
        );
    }
    assert_eq!(m.relations.aux.rollout.files.len(), MAX_ROLLOUT_FILES);
}

#[test]
fn gap_pe_淘汰冷却文件后截止前不重取路径() {
    let (mut m, _) = base();
    files(&m);
    fail_child_path(&mut m);
    let deadline = m.relations.aux.rollout.files[C].next_at;
    assert!(matches!(
        m.relations.aux.rollout.files[C].path,
        RolloutPathState::Failed(V2FallbackReason::RolloutPathUnavailable)
    ));
    assert_eq!(deadline, m.clock.mono_ms + ROLLOUT_RETRY_MS);
    fill_cooling(&mut m, deadline);
    // 新线程要位置：排在最前的冷却文件被淘汰。
    fx::create("丁");
    child_edge(&mut m, "丁");
    let out = interacted_to(&mut m, "乙跟进", "丁");
    assert_eq!(path_requests(&out, "丁"), 1, "新线程发出路径请求");
    assert!(
        !m.relations.aux.rollout.files.contains_key(C),
        "冷却中的「子」被淘汰"
    );
    // 截止前原线程再被跟进：冷却仍有效，不重取路径、仍报原来的原因。
    call_line(&m, "再跟进", "followup_task", C, "再来");
    let out = interacted_to(&mut m, "再跟进", C);
    assert_eq!(path_requests(&out, C), 0, "冷却截止前不重取路径");
    fx::poll(&mut m);
    m.clock.mono_ms += ROLLOUT_WAIT_MS;
    let mut drain = vec![];
    m.drive_rollout(&mut drain);
    assert_eq!(path_requests(&drain, C), 0);
    assert_eq!(
        op(&m, "再跟进").v2_fallback,
        Some(V2FallbackReason::RolloutPathUnavailable),
        "淘汰后冷却仍报原来的原因"
    );
    // 截止过后再跟进：重新取路径一次。
    call_line(&m, "三跟进", "followup_task", C, "又来");
    m.clock.mono_ms = deadline + 1;
    let out = interacted_to(&mut m, "三跟进", C);
    assert_eq!(path_requests(&out, C), 1, "截止过后重新取路径");
}

#[test]
fn gap_pe_冷却保留表满时不淘汰冷却文件() {
    let (mut m, _) = base();
    files(&m);
    fail_child_path(&mut m);
    let epoch = m.relations.conn_epoch;
    let deadline = m.relations.aux.rollout.files[C].next_at;
    fill_cooling(&mut m, deadline);
    // 淘汰的保留表也填满。
    for i in 0..MAX_ROLLOUT_FILES {
        m.relations.aux.rollout.evicted_cooldowns.insert(
            format!("保留{i}"),
            (epoch, deadline, V2FallbackReason::RolloutUnreadable),
        );
    }
    fx::create("丁");
    child_edge(&mut m, "丁");
    let out = interacted_to(&mut m, "乙跟进", "丁");
    assert_eq!(
        path_requests(&out, "丁"),
        0,
        "保留表满：不淘汰冷却文件，新线程本步拿不到位置"
    );
    assert!(m.relations.aux.rollout.files.contains_key(C));
    assert!(m.relations.aux.rollout.files.contains_key(P));
}

#[test]
fn gap_pe_断档后旧路径成功回包不启动读取() {
    let (mut m, _) = base();
    files(&m);
    call_line(&m, "跟进", "followup_task", C, "任务");
    let actions = pending_path(&mut m, "跟进", C);
    let old = path_generation(&actions, C).expect("跟进发出路径请求");
    let good = Ok(json!({"thread":{"id":C,"path":fx::path(C)}}));
    // 断档后到的是旧代次的回包：不得标 Ready、不得启动读取。
    let (next, gap_actions) = step(
        std::mem::take(&mut m),
        Event::ObservationGap {
            thread: Some(C.into()),
        },
    );
    m = next;
    m.rollout_path_reply(C, old, &good);
    assert!(
        !matches!(
            m.relations.aux.rollout.files[C].path,
            RolloutPathState::Ready(_)
        ),
        "旧代次回包不得让 path 变 Ready"
    );
    let mut drain = vec![];
    m.drive_rollout(&mut drain);
    assert!(
        !drain
            .iter()
            .any(|a| matches!(a, Action::ReadRollout(r) if r.thread == C)),
        "不派发读取"
    );
    // 断档本步已按新代次重发请求；新回包照常接纳。
    let new = path_generation(&gap_actions, C).expect("断档后重发路径请求");
    assert_ne!(old, new);
    m.rollout_path_reply(C, new, &good);
    assert!(matches!(
        m.relations.aux.rollout.files[C].path,
        RolloutPathState::Ready(_)
    ));
}

#[test]
fn gap_pe_断档后旧路径失败回包不留冷却() {
    let (mut m, _) = base();
    files(&m);
    call_line(&m, "跟进", "followup_task", C, "任务");
    let actions = pending_path(&mut m, "跟进", C);
    let old = path_generation(&actions, C).expect("跟进发出路径请求");
    let (next, gap_actions) = step(
        std::mem::take(&mut m),
        Event::ObservationGap {
            thread: Some(C.into()),
        },
    );
    m = next;
    m.rollout_path_reply(C, old, &Err(json!({"message": "断档前的失败"})));
    assert!(
        !matches!(
            m.relations.aux.rollout.files[C].path,
            RolloutPathState::Failed(_)
        ),
        "旧代次失败回包不得留下冷却"
    );
    // 线程仍被需要：断档本步已按新代次重发请求。
    let new = path_generation(&gap_actions, C).expect("断档后重发路径请求");
    assert_ne!(old, new);
}

#[test]
fn gap_pe_断档只退休受影响线程的路径请求() {
    let (mut m, _) = base();
    files(&m);
    call_line(&m, "跟进", "followup_task", C, "任务");
    let actions = pending_path(&mut m, "跟进", C);
    let old_c = path_generation(&actions, C).expect("跟进发出路径请求");
    let old_p = path_generation(&actions, P).expect("父线程路径请求");
    let (next, gap_actions) = step(
        std::mem::take(&mut m),
        Event::ObservationGap {
            thread: Some(C.into()),
        },
    );
    m = next;
    // 父线程的请求原样：代次不变、不重发、旧回包仍接纳。
    assert_eq!(
        m.relations.aux.rollout.files[P].path,
        RolloutPathState::Requested(old_p)
    );
    assert!(path_generation(&gap_actions, P).is_none(), "父线程不重发");
    m.rollout_path_reply(P, old_p, &Ok(json!({"thread":{"id":P,"path":fx::path(P)}})));
    assert!(matches!(
        m.relations.aux.rollout.files[P].path,
        RolloutPathState::Ready(_)
    ));
    // 子线程已按新代次重发。
    let new_c = path_generation(&gap_actions, C).expect("断档后子线程重发路径请求");
    assert_ne!(old_c, new_c);
}

#[test]
fn gap_pe_传输断档退休全部在途路径请求() {
    let (mut m, _) = base();
    files(&m);
    call_line(&m, "跟进", "followup_task", C, "任务");
    let actions = pending_path(&mut m, "跟进", C);
    let old_c = path_generation(&actions, C).expect("跟进发出路径请求");
    let old_p = path_generation(&actions, P).expect("父线程路径请求");
    let (next, gap_actions) = step(
        std::mem::take(&mut m),
        Event::ObservationGap { thread: None },
    );
    m = next;
    // 两条线程的旧回包一律不认。
    for (t, old) in [(P, old_p), (C, old_c)] {
        m.rollout_path_reply(t, old, &Ok(json!({"thread":{"id":t,"path":fx::path(t)}})));
        assert!(
            !matches!(
                m.relations.aux.rollout.files[t].path,
                RolloutPathState::Ready(_)
            ),
            "旧代次回包不得让 {t} 的 path 变 Ready"
        );
    }
    // 两条线程都按新代次重发。
    for (t, old) in [(P, old_p), (C, old_c)] {
        let new = path_generation(&gap_actions, t).expect("断档后重发路径请求");
        assert_ne!(old, new);
    }
}

/// W71 的守护：只入队留言的调用已在缓存里，之后父文件进入失败冷却。
/// 留言按父调用类型定为严格，不看父文件此刻读不读得了（调用是本代际此前读到的）。
#[test]
fn gap_pe_父文件冷却不降级已缓存的只入队留言() {
    let (mut m, _) = base();
    files(&m);
    call_line(&m, "预读", "send_message", C, "");
    call_line(&m, "目标", "send_message", C, "任务");
    interacted_to(&mut m, "预读", C);
    assert!(m
        .relations
        .aux
        .rollout
        .calls
        .contains_key(&(P.into(), "目标".into())));
    let epoch = m.relations.conn_epoch;
    m.relations.aux.rollout.files.insert(
        P.into(),
        RolloutFile {
            epoch,
            path: RolloutPathState::Failed(V2FallbackReason::RolloutUnreadable),
            cursor: None,
            reading: None,
            reads: 0,
            next_at: m.clock.mono_ms + ROLLOUT_RETRY_MS,
            floor_dropped: None,
        },
    );
    let reads = m.relations.aux.rollout.reads_issued;
    interacted_to(&mut m, "目标", C);
    assert_eq!(
        m.relations.aux.rollout.reads_issued, reads,
        "不为留言再派读"
    );
    let operation = op(&m, "目标");
    assert!(operation.v2_queue_only, "留言只入队");
    assert!(operation.v2_strict, "父文件冷却不降级留言");
    assert_eq!(operation.v2_fallback, None);
    assert!(operation.receivers.is_empty() && operation.proven_turns.is_empty());
}
