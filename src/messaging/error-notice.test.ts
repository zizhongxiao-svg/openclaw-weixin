import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  send: vi.fn(),
  direct: vi.fn(),
  drain: vi.fn(),
  sdkAvailable: true,
  sdkError: undefined as Error | undefined,
}));
vi.mock("openclaw/plugin-sdk/channel-outbound", () => ({
  get sendDurableMessageBatch() {
    if (mocks.sdkError) throw mocks.sdkError;
    return mocks.sdkAvailable ? mocks.send : undefined;
  },
}));
vi.mock("openclaw/plugin-sdk/infra-runtime", () => ({ drainPendingDeliveries: mocks.drain }));
vi.mock("./send.js", () => ({ sendMessageWeixin: mocks.direct }));
vi.mock("../util/logger.js", () => ({ logger: { debug: vi.fn() } }));
import { flushWeixinErrorNotices, sendWeixinErrorNotice } from "./error-notice.js";

const params = () => ({
  to: "test@im.wechat",
  contextToken: "context",
  message: "Request failed",
  baseUrl: "https://example.invalid",
  accountId: "test-account",
  runId: "test-run",
  config: {},
  errLog: vi.fn(),
});
beforeEach(() => {
  vi.clearAllMocks();
  mocks.sdkAvailable = true;
  mocks.sdkError = undefined;
});

describe("durable error notices", () => {
  it("uses required write-ahead custody with a stable bounded recovery intent", async () => {
    mocks.send.mockImplementation(async (p) => {
      p.onDeliveryIntent();
      return { status: "sent" };
    });
    expect(await sendWeixinErrorNotice(params())).toEqual({ status: "sent" });
    await sendWeixinErrorNotice(params());
    const [first, second] = mocks.send.mock.calls.map(([p]) => p);
    expect(first.deliveryIntentId).toBe(second.deliveryIntentId);
    expect(first).toMatchObject({
      durability: "required",
      maxRetries: 4,
      channel: "openclaw-weixin",
      accountId: "test-account",
    });
    expect(first).not.toHaveProperty("token");
    expect(first).not.toHaveProperty("contextToken");
    expect(mocks.direct).not.toHaveBeenCalled();
  });

  it("reports unresolved durable delivery without a second physical send", async () => {
    mocks.send.mockImplementation(async (p) => {
      p.onDeliveryIntent();
      return { status: "failed" };
    });
    expect(await sendWeixinErrorNotice(params())).toEqual({ status: "pending" });
    expect(mocks.direct).not.toHaveBeenCalled();
  });

  it("does not claim durability when queue creation fails", async () => {
    mocks.send.mockRejectedValue(new Error("queue unavailable"));
    expect(await sendWeixinErrorNotice(params())).toEqual({ status: "failed" });
    expect(mocks.direct).not.toHaveBeenCalled();
  });

  it("preserves policy suppression without direct bypass", async () => {
    mocks.send.mockResolvedValue({ status: "suppressed" });
    expect(await sendWeixinErrorNotice(params())).toEqual({ status: "suppressed" });
    expect(mocks.direct).not.toHaveBeenCalled();
  });

  it("does not emit a notice on channel cancellation", async () => {
    const controller = new AbortController();
    controller.abort();
    expect(await sendWeixinErrorNotice({ ...params(), abortSignal: controller.signal })).toEqual({
      status: "cancelled",
    });
    expect(mocks.send).not.toHaveBeenCalled();
  });

  it("drains only this account's error intents and retains core backoff", async () => {
    mocks.drain.mockResolvedValue(undefined);
    await flushWeixinErrorNotices(params());
    const p = mocks.drain.mock.calls[0][0];
    expect(
      p.selectEntry({
        id: "weixin-error-test",
        channel: "openclaw-weixin",
        accountId: "test-account",
      }),
    ).toEqual({ match: true });
    expect(
      p.selectEntry({ id: "normal", channel: "openclaw-weixin", accountId: "test-account" }),
    ).toEqual({ match: false });
    expect(
      p.selectEntry({
        id: "weixin-error-test",
        channel: "openclaw-weixin",
        accountId: "other-account",
      }),
    ).toEqual({ match: false });
    expect(
      p.selectEntry({ id: "weixin-error-test", channel: "telegram", accountId: "test-account" }),
    ).toEqual({ match: false });
  });

  it("coalesces overlapping poll drains and reports recovery errors", async () => {
    let finish!: () => void;
    mocks.drain.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finish = resolve;
        }),
    );
    const p = params();
    const first = flushWeixinErrorNotices(p);
    const second = flushWeixinErrorNotices(p);
    expect(first).toBe(second);
    await vi.waitFor(() => expect(mocks.drain).toHaveBeenCalledTimes(1));
    finish();
    await first;
    mocks.drain.mockRejectedValue(new Error("offline"));
    await flushWeixinErrorNotices(p);
    expect(p.errLog).toHaveBeenCalledWith(expect.stringContaining("recovery failed"));
  });
});

describe("standalone compatibility sends", () => {
  it("preserves recipient, context, run and optional cancellation on direct calls", async () => {
    mocks.direct.mockResolvedValue({ messageId: "sent" });
    const p = { ...params(), config: undefined };
    expect(await sendWeixinErrorNotice(p)).toEqual({ status: "sent" });
    expect(mocks.direct).toHaveBeenCalledWith({
      to: p.to,
      text: p.message,
      opts: expect.objectContaining({
        contextToken: p.contextToken,
        runId: p.runId,
        accountId: p.accountId,
      }),
    });
    expect(mocks.send).not.toHaveBeenCalled();
  });
  it("retains standalone send support without a context token", async () => {
    mocks.direct.mockResolvedValue({ messageId: "sent" });
    expect(
      await sendWeixinErrorNotice({ ...params(), config: undefined, contextToken: undefined }),
    ).toEqual({ status: "sent" });
    expect(mocks.direct).toHaveBeenCalledTimes(1);
  });
  it("reports direct failure without throwing or claiming a pending durable notice", async () => {
    mocks.direct.mockRejectedValue(new Error("send failed"));
    const p = { ...params(), config: undefined };
    expect(await sendWeixinErrorNotice(p)).toEqual({ status: "failed" });
    expect(p.errLog).toHaveBeenCalledWith(expect.stringContaining("error notice delivery failed"));
  });
});

describe("host capability and recovery boundaries", () => {
  it("uses the compatibility send when an older host lacks the durable export", async () => {
    mocks.sdkAvailable = false;
    vi.resetModules();
    const legacy = await import("./error-notice.js");
    mocks.direct.mockResolvedValue({ messageId: "sent" });
    expect(await legacy.sendWeixinErrorNotice(params())).toEqual({ status: "sent" });
    expect(mocks.direct).toHaveBeenCalledTimes(1);
    expect(mocks.send).not.toHaveBeenCalled();
  });

  it("uses compatibility for an unavailable SDK path but never bypasses other loader errors", async () => {
    mocks.sdkError = Object.assign(new Error("old host"), {
      code: "ERR_PACKAGE_PATH_NOT_EXPORTED",
    });
    vi.resetModules();
    const legacy = await import("./error-notice.js");
    mocks.direct.mockResolvedValue({ messageId: "sent" });
    expect(await legacy.sendWeixinErrorNotice(params())).toEqual({ status: "sent" });
    expect(mocks.direct).toHaveBeenCalledTimes(1);
    mocks.direct.mockClear();
    mocks.sdkError = new Error("SDK initialization failed");
    vi.resetModules();
    const broken = await import("./error-notice.js");
    expect(await broken.sendWeixinErrorNotice(params())).toEqual({ status: "failed" });
    expect(mocks.direct).not.toHaveBeenCalled();
  });

  it("retains pending custody when transport throws after persistence", async () => {
    mocks.send.mockImplementation(async (p) => {
      p.onDeliveryIntent();
      throw new Error("transport unavailable");
    });
    expect(await sendWeixinErrorNotice(params())).toEqual({ status: "pending" });
    expect(mocks.direct).not.toHaveBeenCalled();
  });

  it("does not invent pending custody when the host declines to create an intent", async () => {
    mocks.send.mockResolvedValue({ status: "failed" });
    expect(await sendWeixinErrorNotice(params())).toEqual({ status: "failed" });
    expect(mocks.direct).not.toHaveBeenCalled();
  });

  it("stops recovery admission when the channel is cancelled", async () => {
    const controller = new AbortController();
    const p = { ...params(), abortSignal: controller.signal };
    mocks.drain.mockResolvedValue(undefined);
    await flushWeixinErrorNotices(p);
    const drain = mocks.drain.mock.calls[0][0];
    expect(drain.shouldContinue()).toBe(true);
    controller.abort();
    expect(drain.shouldContinue()).toBe(false);
    await flushWeixinErrorNotices(p);
    expect(mocks.drain).toHaveBeenCalledTimes(1);
  });
});
