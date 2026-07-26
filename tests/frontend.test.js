"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const dashboard = require("../src/dashboard/assets/app.js");
const chart = require("../src/dashboard/assets/chart.js");

test("pagination correction requests the last page exactly once", () => {
  assert.deepEqual(dashboard.paginationDecision(9, 50, 120, false), {
    page: 3,
    totalPages: 3,
    retry: true
  });
  assert.deepEqual(dashboard.paginationDecision(3, 50, 120, true), {
    page: 3,
    totalPages: 3,
    retry: false
  });
});

test("pagination state prevents navigation while loading or beyond known bounds", () => {
  assert.deepEqual(dashboard.paginationControls(2, 4, true), { previous: false, next: false });
  assert.deepEqual(dashboard.paginationControls(4, 4, false), { previous: true, next: false });
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

test("upstream status distinguishes no samples, unavailable, degraded, and healthy", () => {
  assert.deepEqual(dashboard.upstreamStatus({ samples: 0, successes: 0, failure_rate: 0 }), { text: "暂无数据", kind: "neutral" });
  assert.deepEqual(dashboard.upstreamStatus({ samples: 3, successes: 0, failure_rate: 1 }), { text: "不可用", kind: "bad" });
  assert.deepEqual(dashboard.upstreamStatus({ samples: 4, successes: 3, failure_rate: 0.25 }), { text: "有失败", kind: "warn" });
  assert.deepEqual(dashboard.upstreamStatus({ samples: 4, successes: 4, failure_rate: 0 }), { text: "正常", kind: "good" });
});

test("chart values reject non-finite and negative API values", () => {
  assert.equal(chart.normalizeValue(Infinity), 0);
  assert.equal(chart.normalizeValue("not-a-number"), 0);
  assert.equal(chart.normalizeValue(-2), 0);
  assert.equal(chart.normalizeValue("42"), 42);
});

test("chart values use compact labels", () => {
  assert.equal(chart.formatCount(999), "999");
  assert.equal(chart.formatCount(1200), "1.2K");
  assert.equal(chart.formatCount(2500000), "2.5M");
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
