/** broker @cxb2 内容段；身份字段始终使用完整线程 ID。 */
export interface TreeLogPayload {
  eventId: string;
  rootThreadId: string;
  threadId: string;
  parentThreadId: string | null;
  shortId: string | null;
  sourceRole: "run" | "execution" | "subagent";
  turnId: string | null;
  itemId: string | null;
  summary: string;
  truncated: boolean;
  /** 暂存摘要的稳定身份：同一摘要在各候选运行的副本和迟到确认补写共用同一个值；直接写入的行没有此字段。 */
  summaryId?: string;
  late?: boolean;
  attribution?: "unconfirmed";
  coverageIssue?: "beforeBindingUnknown" | "discoveryIncomplete";
}

/** 静态时间线项；旧行及文件级标记缺少的身份保持未知。 */
export interface TreeLogEvent {
  eventId: string | null;
  ts: string;
  runId: string | null;
  rootThreadId: string | null;
  threadId: string | null;
  parentThreadId: string | null;
  shortId: string | null;
  sourceRole: TreeLogPayload["sourceRole"] | "legacy" | "log";
  turnId: string | null;
  itemId: string | null;
  kind: string;
  text: string;
  summary: string;
  truncated: boolean;
  late?: boolean;
  attribution?: TreeLogPayload["attribution"];
}

export interface LogCoverageIssue {
  code: "legacySourceUnknown" | "rotationWindow" | "droppedLines" | "beforeBindingUnknown"
    | "discoveryIncomplete" | "metadataUnavailable" | "missingLog" | "trailingPartialLine";
  runId?: string;
  threadId?: string;
}

export interface LogCoverage {
  scope: "retained";
  issues: LogCoverageIssue[];
}
