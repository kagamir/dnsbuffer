(function (root, factory) {
  "use strict";
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.DnsDashboard = api;
  if (typeof document !== "undefined") {
    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", api.start, { once: true });
    else api.start();
  }
}(typeof window !== "undefined" ? window : globalThis, function () {
  "use strict";

  function normalizeCount(value) {
    const number = Number(value);
    return Number.isFinite(number) && number >= 0 ? number : 0;
  }

  function queryResponseDecision(requestedPage, currentPage, pageSize, total, corrected) {
    const totalPages = Math.max(1, Math.ceil(normalizeCount(total) / pageSize));
    if (requestedPage !== currentPage) return { page: currentPage, totalPages, action: "ignore" };
    if (requestedPage <= totalPages) return { page: requestedPage, totalPages, action: "render" };
    return { page: totalPages, totalPages, action: corrected ? "reject" : "retry" };
  }

  function applyQueryResponse(state, requestedPage, pageSize, total, corrected) {
    const decision = queryResponseDecision(requestedPage, state.requestedPage, pageSize, total, corrected);
    if (decision.action === "retry") state.requestedPage = decision.page;
    if (decision.action === "render") {
      state.page = decision.page;
      state.totalPages = decision.totalPages;
    }
    return decision;
  }

  function paginationControls(page, totalPages, loading) {
    return {
      previous: !loading && page > 1,
      next: !loading && (totalPages == null || page < totalPages)
    };
  }

  function mayFinishQuery(requestId, activeRequestId) {
    return requestId === activeRequestId;
  }

  function searchDecision(value) {
    return { search: value.trim(), page: 1, totalPages: null };
  }

  function createRoundTracker(names) {
    let activeId = 0;
    let results = new Map();
    return {
      begin() { activeId += 1; results = new Map(); return activeId; },
      record(id, name, result) {
        if (id !== activeId || !names.includes(name)) return null;
        results.set(name, result);
        const complete = results.size === names.length;
        const failed = [...results.values()].filter((value) => value === "failure" || value === false).length;
        const superseded = [...results.values()].filter((value) => value === "superseded").length;
        return { id, complete, success: complete && failed === 0 && superseded === 0, failed, superseded };
      }
    };
  }

  function classifyRegionResult({ aborted, current, failed }) {
    if (aborted || !current) return "superseded";
    return failed ? "failure" : "success";
  }

  function mapQueryResult(regionResult, action) {
    if (regionResult !== "success") return regionResult;
    return action === "render" ? "success" : "superseded";
  }

  function averageResponseText(data) {
    const samples = normalizeCount(data && data.samples);
    const avg = Number(data && data.avg_ms);
    if (samples === 0 || !Number.isFinite(avg) || avg < 0) return "--";
    return `${avg >= 100 ? Math.round(avg) : avg.toFixed(1)} ms`;
  }

  function upstreamStatus(upstream) {
    const samples = normalizeCount(upstream.samples);
    const successes = normalizeCount(upstream.successes);
    const failureRate = Math.max(0, Number(upstream.failure_rate) || 0);
    if (samples === 0) return { text: "No data", kind: "neutral" };
    if (successes === 0) return { text: "Unavailable", kind: "bad" };
    if (failureRate > 0 || successes < samples) return { text: "Has failures", kind: "warn" };
    return { text: "Healthy", kind: "good" };
  }

  function start() {
    if (start.started) return;
    start.started = true;
    const state = {
      page: 1, pageSize: 50, search: "", controllers: new Map(),
      queryLoading: false, totalPages: null, requestedPage: 1, trendData: null,
      cacheData: null, queryRequestId: 0
    };
    const regionNames = ["trend", "cache", "upstreams", "rankings", "queries", "response"];
    const roundTracker = createRoundTracker(regionNames);
    const regions = Object.fromEntries(regionNames.map((name) => [name, document.querySelector(`#${name}`)]));
    const previousButton = document.querySelector("#previous-page");
    const nextButton = document.querySelector("#next-page");
    const refreshButton = document.querySelector("#refresh-button");

    function element(tag, text, className) {
      const node = document.createElement(tag);
      if (text !== undefined) node.textContent = String(text);
      if (className) node.className = className;
      return node;
    }
    function replaceChildren(target, children) { target.replaceChildren(...children); }
    function setError(name, message) {
      const error = regions[name].querySelector(".panel-error");
      error.textContent = message || "";
      error.classList.toggle("is-visible", Boolean(message));
    }
    function updatePagination() {
      const controls = paginationControls(state.page, state.totalPages, state.queryLoading);
      previousButton.disabled = !controls.previous;
      nextButton.disabled = !controls.next;
    }
    function setQueryLoading(loading) {
      state.queryLoading = loading;
      regions.queries.setAttribute("aria-busy", String(loading));
      updatePagination();
    }
    function beginRefreshStatus() {
      const status = document.querySelector("#refresh-status");
      const dot = document.querySelector(".status-dot");
      refreshButton.disabled = true;
      status.removeAttribute("aria-live");
      status.textContent = "Updating";
      dot.className = "status-dot is-loading";
    }
    function finishRefreshStatus(result) {
      if (!result?.complete) return;
      refreshButton.disabled = false;
      const status = document.querySelector("#refresh-status");
      const dot = document.querySelector(".status-dot");
      status.textContent = result.success ? "All sections updated" : result.failed ? `${result.failed} section(s) failed to update` : "Some sections were superseded by a newer request";
      if (result.success) {
        status.removeAttribute("aria-live");
        dot.className = "status-dot";
        document.querySelector("#last-updated").textContent = `Last success: ${new Date().toLocaleTimeString()}`;
      } else {
        status.setAttribute("aria-live", "assertive");
        dot.className = "status-dot has-error";
      }
    }
    async function loadRegion(name, url, render, report) {
      // report provides a custom failure message for small sections without a .panel-error (e.g. the average response time in the heading)
      const notify = report || ((message) => setError(name, message));
      state.controllers.get(name)?.abort();
      const controller = new AbortController();
      state.controllers.set(name, controller);
      try {
        const response = await fetch(url, { signal: controller.signal });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        if (state.controllers.get(name) !== controller) return "superseded";
        await render(data);
        notify("");
        return "success";
      } catch (error) {
        const result = classifyRegionResult({ aborted: error.name === "AbortError", current: state.controllers.get(name) === controller, failed: true });
        if (result === "failure") {
          notify("Update failed; keeping the previous data");
        }
        return result;
      } finally {
        if (state.controllers.get(name) === controller) state.controllers.delete(name);
      }
    }
    function renderTrend(data) {
      const buckets = Array.isArray(data.buckets) ? data.buckets : [];
      const start = new Date(data.start);
      const end = new Date(data.end);
      const aggregation = data.granularity === "day" ? "Aggregated by day" : "Aggregated by hour";
      if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) throw new Error("invalid trend range");
      state.trendData = { buckets, granularity: data.granularity };
      window.DnsTrendChart.render(document.querySelector("#trend-chart"), buckets, data.granularity);
      document.querySelector("#trend-range").textContent = `${aggregation} · ${start.toLocaleString()} to ${end.toLocaleString()}`;
      const totals = buckets.reduce((sum, bucket) => ({
        total: sum.total + window.DnsTrendChart.normalizeValue(bucket.total_queries),
        blocked: sum.blocked + window.DnsTrendChart.normalizeValue(bucket.blocked_queries),
        cache: sum.cache + window.DnsTrendChart.normalizeValue(bucket.cache_hits)
      }), { total: 0, blocked: 0, cache: 0 });
      document.querySelector("#trend-summary").textContent = `${aggregation}: ${window.DnsTrendChart.formatCount(totals.total)} queries, ${window.DnsTrendChart.formatCount(totals.blocked)} blocked, ${window.DnsTrendChart.formatCount(totals.cache)} cache hits`;
    }
    function renderCache(data) {
      const points = Array.isArray(data.points) ? data.points : [];
      state.cacheData = data;
      window.DnsTrendChart.renderCacheCurve(document.querySelector("#cache-chart"), data);
      const format = window.DnsTrendChart.formatCount;
      if (!points.length || !data.current) {
        document.querySelector("#cache-note").textContent = "Collecting observations (one recorded per request)";
        document.querySelector("#cache-summary").textContent = "No observations yet for the cache hit rate curve";
        return;
      }
      const percent = `${(Math.min(1, Math.max(0, Number(data.current.hit_rate) || 0)) * 100).toFixed(1)}%`;
      document.querySelector("#cache-note").textContent =
        `${points.length} observations · current ${format(data.current_entries)} / ${format(data.max_entries)} entries · cumulative hit rate ${percent}`;
      document.querySelector("#cache-summary").textContent =
        `Recorded ${points.length} hit rate observations by cache entry count; current cache holds ${format(data.current_entries)} entries (configured limit ${format(data.max_entries)}), cumulative hit rate ${percent}`;
    }
    function renderUpstreams(data) {
      const target = document.querySelector("#upstream-list");
      if (!Array.isArray(data) || data.length === 0) return replaceChildren(target, [element("p", "No upstream data", "empty")]);
      replaceChildren(target, data.map((upstream) => {
        const card = element("article", undefined, "upstream-card");
        const heading = element("div", undefined, "upstream-heading");
        const names = element("div");
        names.append(element("strong", upstream.name), element("span", upstream.group, "muted"));
        const status = upstreamStatus(upstream);
        heading.append(names, element("span", status.text, `badge badge-${status.kind}`));
        const metrics = element("dl", undefined, "metrics");
        const latency = Number(upstream.avg_latency_ms);
        [["Avg latency", Number.isFinite(latency) ? `${latency.toFixed(1)} ms` : "--"], ["Samples", upstream.samples], ["Failure rate", `${((Number(upstream.failure_rate) || 0) * 100).toFixed(1)}%`]].forEach(([label, value]) => {
          const group = element("div"); group.append(element("dt", label), element("dd", value)); metrics.append(group);
        });
        card.append(heading, metrics); return card;
      }));
    }
    function renderResponseTime(data) {
      document.querySelector("#avg-response-value").textContent = averageResponseText(data);
    }
    function reportResponseError(message) {
      document.querySelector("#avg-response-value").classList.toggle("has-error", Boolean(message));
    }
    function emptyRow(columns, message) {
      const row = element("tr"); const cell = element("td", message, "empty"); cell.colSpan = columns; row.append(cell); return row;
    }
    function renderRankings(data) {
      const body = document.querySelector("#ranking-body");
      if (!Array.isArray(data) || data.length === 0) return replaceChildren(body, [emptyRow(4, "No domain rankings")]);
      replaceChildren(body, data.map((record, index) => {
        const row = element("tr"); const domain = element("td");
        domain.append(element("span", String(index + 1).padStart(2, "0"), "rank"), element("span", record.domain));
        row.append(domain, element("td", record.total_queries), element("td", record.blocked_queries), element("td", record.cache_hits)); return row;
      }));
    }
    function addBadge(target, text, kind) { target.append(element("span", text, `badge ${kind}`)); }
    function renderQueries(data) {
      const body = document.querySelector("#query-body"); const records = Array.isArray(data.records) ? data.records : [];
      if (!records.length) replaceChildren(body, [emptyRow(5, state.search ? "No matching query records" : "No query records")]);
      else replaceChildren(body, records.map((record) => {
        const row = element("tr"); const date = new Date(record.timestamp); const domain = element("td");
        domain.append(element("strong", record.domain), element("span", record.query_type, "muted block"));
        const ips = element("td"); const responseIps = Array.isArray(record.response_ips) ? record.response_ips : [];
        if (!responseIps.length) ips.append(element("span", "--", "muted")); else responseIps.forEach((ip) => ips.append(element("code", ip)));
        const result = element("td", undefined, "result-cell"); addBadge(result, record.response_code || "UNKNOWN", "badge-neutral");
        if (record.blocked) addBadge(result, "Blocked", "badge-blocked"); if (record.cache_hit) addBadge(result, "Cache", "badge-cache");
        row.append(element("td", Number.isNaN(date.getTime()) ? "--" : date.toLocaleString()), domain, ips, element("td", `${record.duration_ms} ms`), result); return row;
      }));
      document.querySelector("#page-status").textContent = `Page ${state.page} / ${state.totalPages} · ${window.DnsTrendChart.formatCount(data.total)} records`;
      updatePagination();
    }
    async function loadQueries(corrected, roundId) {
      const requestId = ++state.queryRequestId;
      setQueryLoading(true);
      if (!corrected) state.requestedPage = state.page;
      const requestedPage = state.requestedPage;
      const query = `page=${requestedPage}&page_size=${state.pageSize}&search=${encodeURIComponent(state.search)}`;
      let action = "ignore";
      const success = await loadRegion("queries", `/api/dashboard/queries?${query}`, (data) => {
        const decision = applyQueryResponse(state, requestedPage, state.pageSize, data.total, corrected);
        action = decision.action;
        if (action === "render") renderQueries(data);
      });
      if (success === "success" && action === "retry") return loadQueries(true, roundId);
      if (success === "success" && action === "reject") setError("queries", "The total count kept changing; keeping the previous query results");
      if (mayFinishQuery(requestId, state.queryRequestId)) setQueryLoading(false);
      return mapQueryResult(success, action);
    }
    function refreshAll() {
      const roundId = roundTracker.begin();
      beginRefreshStatus();
      const requests = [
        ["trend", loadRegion("trend", "/api/dashboard/trend", renderTrend)],
        ["cache", loadRegion("cache", "/api/dashboard/cache-curve", renderCache)],
        ["upstreams", loadRegion("upstreams", "/api/dashboard/upstreams", renderUpstreams)],
        ["response", loadRegion("response", "/api/dashboard/response-time", renderResponseTime, reportResponseError)],
        ["rankings", loadRegion("rankings", "/api/dashboard/rankings", renderRankings)],
        ["queries", loadQueries(false, roundId)]
      ];
      return Promise.allSettled(requests.map(([name, request]) => request.then((result) => {
        finishRefreshStatus(roundTracker.record(roundId, name, result));
        return result;
      })));
    }
    let searchTimer;
    document.querySelector("#query-search").addEventListener("input", (event) => {
      window.clearTimeout(searchTimer); const value = event.target.value;
      searchTimer = window.setTimeout(() => {
        Object.assign(state, searchDecision(value)); loadQueries(false);
      }, 300);
    });
    previousButton.addEventListener("click", () => { if (!state.queryLoading && state.page > 1) { state.page -= 1; loadQueries(false); } });
    nextButton.addEventListener("click", () => { if (!state.queryLoading && (state.totalPages == null || state.page < state.totalPages)) { state.page += 1; loadQueries(false); } });
    let resizeTimer;
    window.addEventListener("resize", () => { window.clearTimeout(resizeTimer); resizeTimer = window.setTimeout(() => {
      if (state.trendData) window.DnsTrendChart.render(document.querySelector("#trend-chart"), state.trendData.buckets, state.trendData.granularity);
      if (state.cacheData) window.DnsTrendChart.renderCacheCurve(document.querySelector("#cache-chart"), state.cacheData);
    }, 120); });
    // Manual refresh: no periodic polling; the server only computes each section's stats when the user clicks, reducing server load
    refreshButton.addEventListener("click", refreshAll);
    updatePagination(); refreshAll();
  }

  return { start, queryResponseDecision, applyQueryResponse, paginationControls, mayFinishQuery, searchDecision, createRoundTracker, classifyRegionResult, mapQueryResult, upstreamStatus, averageResponseText };
}));
