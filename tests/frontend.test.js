"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");

const dashboard = require("../src/dashboard/assets/app.js");
const chart = require("../src/dashboard/assets/chart.js");

test("pagination orchestration rejects records after two consecutive total reductions", () => {
  assert.deepEqual(dashboard.queryResponseDecision(9, 9, 50, 120, false), {
    page: 3,
    totalPages: 3,
    action: "retry"
  });
  assert.deepEqual(dashboard.queryResponseDecision(3, 3, 50, 20, true), {
    page: 1,
    totalPages: 1,
    action: "reject"
  });
});

test("pagination response only renders for its current requested page", () => {
  assert.equal(dashboard.queryResponseDecision(2, 3, 50, 200, false).action, "ignore");
  assert.equal(dashboard.queryResponseDecision(3, 3, 50, 200, false).action, "render");
});

test("pagination state is committed only by a renderable response", () => {
  const state = { page: 9, totalPages: 9, requestedPage: 9 };
  const first = dashboard.applyQueryResponse(state, 9, 50, 120, false);
  assert.equal(first.action, "retry");
  assert.deepEqual(state, { page: 9, totalPages: 9, requestedPage: 3 });

  const second = dashboard.applyQueryResponse(state, 3, 50, 20, true);
  assert.equal(second.action, "reject");
  assert.deepEqual(state, { page: 9, totalPages: 9, requestedPage: 3 });
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
    failed: 1,
    superseded: 0
  });
});

test("superseded polling query makes the round incomplete without an API failure", () => {
  const tracker = dashboard.createRoundTracker(["trend", "upstreams", "rankings", "queries"]);
  const round = tracker.begin();
  tracker.record(round, "trend", "success");
  tracker.record(round, "upstreams", "success");
  tracker.record(round, "rankings", "success");
  assert.deepEqual(tracker.record(round, "queries", "superseded"), {
    id: round,
    complete: true,
    success: false,
    failed: 0,
    superseded: 1
  });
});

test("only a real request error is classified as failure", () => {
  assert.equal(dashboard.classifyRegionResult({ aborted: true, current: false }), "superseded");
  assert.equal(dashboard.classifyRegionResult({ aborted: false, current: false }), "superseded");
  assert.equal(dashboard.classifyRegionResult({ aborted: false, current: true, failed: true }), "failure");
  assert.equal(dashboard.classifyRegionResult({ aborted: false, current: true, failed: false }), "success");
});

test("query outcome maps reject to superseded, render to success, and preserves HTTP failure", () => {
  assert.equal(dashboard.mapQueryResult("success", "reject"), "superseded");
  assert.equal(dashboard.mapQueryResult("success", "render"), "success");
  assert.equal(dashboard.mapQueryResult("failure", "render"), "failure");
});

test("upstream status distinguishes no samples, unavailable, degraded, and healthy", () => {
  assert.deepEqual(dashboard.upstreamStatus({ samples: 0, successes: 0, failure_rate: 0 }), { text: "No data", kind: "neutral" });
  assert.deepEqual(dashboard.upstreamStatus({ samples: 3, successes: 0, failure_rate: 1 }), { text: "Unavailable", kind: "bad" });
  assert.deepEqual(dashboard.upstreamStatus({ samples: 4, successes: 3, failure_rate: 0.25 }), { text: "Has failures", kind: "warn" });
  assert.deepEqual(dashboard.upstreamStatus({ samples: 4, successes: 4, failure_rate: 0 }), { text: "Healthy", kind: "good" });
});

test("average response time formats milliseconds and degrades to placeholder", () => {
  assert.equal(dashboard.averageResponseText({ samples: 3, avg_ms: 16.333 }), "16.3 ms");
  assert.equal(dashboard.averageResponseText({ samples: 10, avg_ms: 234.6 }), "235 ms");
  assert.equal(dashboard.averageResponseText({ samples: 0, avg_ms: null }), "--");
  assert.equal(dashboard.averageResponseText({ samples: 2, avg_ms: "broken" }), "--");
  assert.equal(dashboard.averageResponseText(undefined), "--");
});

test("chart counts use safe finite number normalization", () => {
  assert.equal(chart.normalizeValue(Infinity), 0);
  assert.equal(chart.normalizeValue("not-a-number"), 0);
  assert.equal(chart.normalizeValue(-2), 0);
  assert.equal(chart.normalizeValue(42), 42);
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

test("hover maps pointer position to the nearest bucket and ignores outside hits", () => {
  assert.equal(chart.hoverIndex(40, 40, 300, 4), 0);
  assert.equal(chart.hoverIndex(340, 40, 300, 4), 3);
  assert.equal(chart.hoverIndex(160, 40, 300, 4), 1);
  assert.equal(chart.hoverIndex(200, 40, 300, 1), 0, "with a single bucket, any position inside the plot area hits it");
  assert.equal(chart.hoverIndex(20, 40, 300, 4), null, "no hit past the tolerance on the left of the plot area");
  assert.equal(chart.hoverIndex(360, 40, 300, 4), null, "no hit past the tolerance on the right of the plot area");
  assert.equal(chart.hoverIndex(NaN, 40, 300, 4), null);
  assert.equal(chart.hoverIndex(100, 40, 300, 0), null);
});

test("hover tooltip lists the three series values in legend order and colors", () => {
  const rows = chart.tooltipRows({ total_queries: 120, blocked_queries: 8, cache_hits: "44" });
  assert.deepEqual(rows.map((row) => [row.label, row.value]), [
    ["All queries", 120],
    ["Blocked", 8],
    ["Cache hits", 44]
  ]);
  assert.deepEqual(rows.map((row) => row.color), chart.series.map((item) => item.color));
  assert.deepEqual(chart.tooltipRows(undefined).map((row) => row.value), [0, 0, 0]);
});

test("cache bars model current entries and hit rate against their own axes", () => {
  const model = chart.cacheBars({
    current_entries: 640,
    max_entries: 1000,
    current: { hit_rate: 0.732 }
  });
  assert.equal(model.countAxisMax, 1000, "count axis stretches to a nearby configured capacity");
  assert.equal(model.hasRate, true);
  const [count, rate] = model.bars;
  assert.deepEqual([count.label, count.display], ["Entries", "640"]);
  assert.equal(count.ratio, 0.64, "count bar fills relative to the configured capacity");
  assert.deepEqual([rate.label, rate.display], ["Hit rate", "73.2%"]);
  assert.equal(rate.ratio, 0.732);
});

test("cache bars degrade gracefully with no observation and clamp the hit rate", () => {
  const empty = chart.cacheBars({ current_entries: 0, max_entries: 500, current: null });
  assert.equal(empty.hasRate, false);
  assert.deepEqual(empty.bars.map((bar) => bar.display), ["0", "--"]);
  assert.equal(empty.bars[1].ratio, 0);

  const clamped = chart.cacheBars({ current_entries: 12, max_entries: 100, current: { hit_rate: 1.4 } });
  assert.equal(clamped.bars[1].ratio, 1, "an out-of-range hit rate is clamped to 100%");
  assert.equal(clamped.bars[1].display, "100.0%");
});

test("cache curve axis follows observations and only stretches to a nearby configured capacity", () => {
  const points = [
    { size: 120, hit_rate: 0.4 },
    { size: 800, hit_rate: 0.7 }
  ];
  assert.equal(chart.cacheCurveAxisMax(points, 1000), 1000, "a configured capacity within 10x is included in the axis range");
  assert.equal(chart.cacheCurveAxisMax(points, 10000), 800, "the axis is not stretched when the configured capacity far exceeds the observations");
  assert.equal(chart.cacheCurveAxisMax(points, 0), 800);
  assert.equal(chart.cacheCurveAxisMax([{ size: 1234, hit_rate: 1 }], 0), 2000, "the upper bound is rounded up to a readable tick");
  assert.equal(chart.cacheCurveAxisMax([], 0), 1, "falls back to 1 when there is no data");
});
