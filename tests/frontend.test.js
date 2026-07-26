"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const dashboard = require("../src/dashboard/assets/app.js");
const chart = require("../src/dashboard/assets/chart.js");

test("pagination orchestration rejects records after two consecutive total reductions", () => {
  assert.deepEqual(dashboard.queryResponseDecision(9, 9, 50, "120", false), {
    page: 3,
    totalPages: 3n,
    action: "retry"
  });
  assert.deepEqual(dashboard.queryResponseDecision(3, 3, 50, "20", true), {
    page: 1,
    totalPages: 1n,
    action: "reject"
  });
});

test("pagination response only renders for its current requested page", () => {
  assert.equal(dashboard.queryResponseDecision(2, 3, 50, "200", false).action, "ignore");
  assert.equal(dashboard.queryResponseDecision(3, 3, 50, "200", false).action, "render");
});

test("pagination state is committed only by a renderable response", () => {
  const state = { page: 9, totalPages: 9n, requestedPage: 9 };
  const first = dashboard.applyQueryResponse(state, 9, 50, "120", false);
  assert.equal(first.action, "retry");
  assert.deepEqual(state, { page: 9, totalPages: 9n, requestedPage: 3 });

  const second = dashboard.applyQueryResponse(state, 3, 50, "20", true);
  assert.equal(second.action, "reject");
  assert.deepEqual(state, { page: 9, totalPages: 9n, requestedPage: 3 });
});

test("pagination state prevents navigation while loading or beyond known bounds", () => {
  assert.deepEqual(dashboard.paginationControls(2, 4n, true), { previous: false, next: false });
  assert.deepEqual(dashboard.paginationControls(4, 4n, false), { previous: true, next: false });
  assert.deepEqual(dashboard.paginationControls(1, null, false), { previous: false, next: true });
});

test("only the latest query request may clear loading state", () => {
  assert.equal(dashboard.mayFinishQuery(4, 5), false);
  assert.equal(dashboard.mayFinishQuery(5, 5), true);
});

test("debounced search trims input and resets pagination knowledge", () => {
  assert.deepEqual(dashboard.searchDecision("  example.com  "), {
    search: "example.com",
    page: 1,
    totalPages: null
  });
});

test("refresh rounds do not mix overlapping results or interactive queries", () => {
  const tracker = dashboard.createRoundTracker(["trend", "upstreams", "rankings", "queries"]);
  const first = tracker.begin();
  tracker.record(first, "trend", true);
  const second = tracker.begin();
  assert.equal(tracker.record(first, "upstreams", false), null);
  assert.equal(tracker.record(undefined, "queries", true), null);
  tracker.record(second, "trend", true);
  tracker.record(second, "upstreams", true);
  tracker.record(second, "rankings", true);
  assert.deepEqual(tracker.record(second, "queries", false), {
    id: second,
    complete: true,
    success: false,
    failed: 1
  });
});

test("upstream status distinguishes no samples, unavailable, degraded, and healthy", () => {
  assert.deepEqual(dashboard.upstreamStatus({ samples: "0", successes: "0", failure_rate: 0 }), { text: "暂无数据", kind: "neutral" });
  assert.deepEqual(dashboard.upstreamStatus({ samples: "3", successes: "0", failure_rate: 1 }), { text: "不可用", kind: "bad" });
  assert.deepEqual(dashboard.upstreamStatus({ samples: "4", successes: "3", failure_rate: 0.25 }), { text: "有失败", kind: "warn" });
  assert.deepEqual(dashboard.upstreamStatus({ samples: "4", successes: "4", failure_rate: 0 }), { text: "正常", kind: "good" });
});

test("chart count parsing preserves decimal string precision", () => {
  assert.equal(chart.parseCount("9007199254740993123"), 9007199254740993123n);
  assert.equal(chart.parseCount(Infinity), 0n);
  assert.equal(chart.parseCount("not-a-number"), 0n);
  assert.equal(chart.parseCount("-2"), 0n);
});

test("chart values use compact labels", () => {
  assert.equal(chart.formatCount(999n), "999");
  assert.equal(chart.formatCount(1200n), "1.2K");
  assert.equal(chart.formatCount(2500000n), "2.5M");
});

test("exact summaries retain every count digit", () => {
  assert.equal(chart.exactCount("9007199254740993123"), "9007199254740993123");
});

test("hour and day buckets have distinct local date-aware labels", () => {
  const timestamp = "2025-07-26T00:00:00Z";
  const hour = chart.formatBucketLabel(timestamp, "hour", "en-CA");
  const day = chart.formatBucketLabel(timestamp, "day", "en-CA");
  assert.match(hour, /\d{1,2}:\d{2}/);
  assert.doesNotMatch(day, /:/);
  assert.notEqual(hour, day);
});

test("three chart series expose non-color line patterns", () => {
  assert.deepEqual(chart.series.map((item) => item.dash), [[], [7, 5], [2, 4]]);
});
