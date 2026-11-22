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
