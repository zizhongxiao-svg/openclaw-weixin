import { createHash, randomUUID } from "node:crypto";
import type { OpenClawConfig } from "openclaw/plugin-sdk/core";

import { logger } from "../util/logger.js";
import { sendMessageWeixin } from "./send.js";

const INTENT_PREFIX = "weixin-error-";
const MAX_RECOVERY_ATTEMPTS = 4;

type NoticeParams = {
  to: string;
  contextToken: string | undefined;
  message: string;
  baseUrl: string;
  token?: string;
  runId?: string;
  accountId?: string;
  config?: OpenClawConfig;
  abortSignal?: AbortSignal;
  errLog: (m: string) => void;
};

export type ErrorNoticeResult = {
  status: "sent" | "pending" | "failed" | "suppressed" | "cancelled";
};

// Older supported hosts have no durable message SDK. Feature-detect it instead
// of making the plugin unloadable on those hosts through a static named import.
type DurableSend = (params: {
  cfg: OpenClawConfig;
  channel: string;
  to: string;
  accountId: string;
  payloads: { text: string }[];
  durability: "required";
  runId?: string;
  deliveryIntentId: string;
  completionRetention: { idPrefix: string; maxAgeMs: number; maxEntries: number };
  maxRetries: number;
  signal?: AbortSignal;
  onDeliveryIntent: () => void;
}) => Promise<{ status: string }>;

type Drain = (params: {
  drainKey: string;
  logLabel: string;
  cfg: OpenClawConfig;
  log: { info: (s: string) => void; warn: (s: string) => void; error: (s: string) => void };
  selectEntry: (entry: { id: string; channel: string; accountId?: string }) => { match: boolean };
  shouldContinue: () => boolean;
}) => Promise<void>;

let durableSendPromise: Promise<DurableSend | undefined> | undefined;
function loadDurableSend(): Promise<DurableSend | undefined> {
  return (durableSendPromise ??= discoverDurableSend());
}

async function discoverDurableSend(): Promise<DurableSend | undefined> {
  try {
    const sdk = (await import("openclaw/plugin-sdk/channel-outbound")) as unknown as {
      sendDurableMessageBatch?: DurableSend;
    };
    return sdk.sendDurableMessageBatch;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ERR_PACKAGE_PATH_NOT_EXPORTED" || code === "ERR_MODULE_NOT_FOUND") return;
    throw error;
  }
}

/** Persist an error notice with the core delivery owner before attempting I/O. */
export async function sendWeixinErrorNotice(params: NoticeParams): Promise<ErrorNoticeResult> {
  if (params.abortSignal?.aborted) return { status: "cancelled" };
  let persisted = false;
  try {
    const sendDurable = params.config && params.accountId ? await loadDurableSend() : undefined;
    if (sendDurable && params.config && params.accountId) {
      const intentId =
        INTENT_PREFIX +
        createHash("sha256")
          .update(JSON.stringify([params.accountId, params.to, params.runId ?? randomUUID()]))
          .digest("hex");
      const result = await sendDurable({
        cfg: params.config,
        channel: "openclaw-weixin",
        to: params.to,
        accountId: params.accountId,
        payloads: [{ text: params.message }],
        durability: "required",
        runId: params.runId,
        deliveryIntentId: intentId,
        completionRetention: {
          idPrefix: INTENT_PREFIX,
          maxAgeMs: 7 * 24 * 60 * 60_000,
          maxEntries: 1024,
        },
        maxRetries: MAX_RECOVERY_ATTEMPTS,
        signal: params.abortSignal,
        onDeliveryIntent: () => {
          persisted = true;
        },
      });
      if (result.status === "sent") return { status: "sent" };
      if (result.status === "suppressed") {
        params.errLog("[weixin] error notice suppressed by delivery policy");
        return { status: "suppressed" };
      }
      params.errLog(`[weixin] error notice delivery unresolved; durableIntent=${persisted}`);
      return { status: persisted ? "pending" : "failed" };
    }
    // Preserve compatibility with older hosts, but never describe this path as
    // durable. Current hosts always use the queue above, including on failure.
    params.errLog("[weixin] durable error delivery unavailable; using direct compatibility send");
    await sendMessageWeixin({
      to: params.to,
      text: params.message,
      opts: {
        baseUrl: params.baseUrl,
        token: params.token,
        contextToken: params.contextToken,
        accountId: params.accountId,
        runId: params.runId,
        abortSignal: params.abortSignal,
      },
    });
    return { status: "sent" };
  } catch (error) {
    params.errLog(
      `[weixin] error notice delivery failed; durableIntent=${persisted}: ${String(error)}`,
    );
    return { status: persisted ? "pending" : "failed" };
  }
}

let drainPromise: Promise<Drain | undefined> | undefined;
async function discoverDrain(): Promise<Drain | undefined> {
  try {
    const sdk = (await import("openclaw/plugin-sdk/infra-runtime")) as unknown as {
      drainPendingDeliveries?: Drain;
    };
    return sdk.drainPendingDeliveries;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === "ERR_PACKAGE_PATH_NOT_EXPORTED" || code === "ERR_MODULE_NOT_FOUND") return;
    throw error;
  }
}

const activeDrains = new Map<string, Promise<void>>();

/** Called by the existing monitor on startup and successful polls, never a new timer. */
export function flushWeixinErrorNotices(params: {
  config: OpenClawConfig;
  accountId: string;
  abortSignal?: AbortSignal;
  errLog: (m: string) => void;
}): Promise<void> {
  if (params.abortSignal?.aborted) return Promise.resolve();
  const existing = activeDrains.get(params.accountId);
  if (existing) return existing;
  const task = (async () => {
    const drain = await (drainPromise ??= discoverDrain());
    if (!drain) return;
    await drain({
      drainKey: `weixin-error:${params.accountId}`,
      logLabel: "weixin-error",
      cfg: params.config,
      log: { info: (s) => logger.debug(s), warn: params.errLog, error: params.errLog },
      selectEntry: (entry) => ({
        match:
          entry.channel === "openclaw-weixin" &&
          entry.accountId === params.accountId &&
          entry.id.startsWith(INTENT_PREFIX),
      }),
      shouldContinue: () => !params.abortSignal?.aborted,
    });
  })()
    .catch((error) => {
      params.errLog(`[weixin] error notice recovery failed: ${String(error)}`);
    })
    .finally(() => {
      activeDrains.delete(params.accountId);
    });
  activeDrains.set(params.accountId, task);
  return task;
}
