(function (root, factory) {
  "use strict";
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.DnsDashboard = api;
  if (typeof document !== "undefined") api.start();
}(typeof window !== "undefined" ? window : globalThis, function () {
  "use strict";

  function paginationDecision(page, pageSize, total, corrected) {
    const safeTotal = Math.max(0, Number(total) || 0);
    const totalPages = Math.max(1, Math.ceil(safeTotal / pageSize));
    const target = Math.min(Math.max(1, page), totalPages);
    return { page: target, totalPages, retry: target !== page && !corrected };
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

  function upstreamStatus(upstream) {
    const samples = Math.max(0, Number(upstream.samples) || 0);
    const successes = Math.max(0, Number(upstream.successes) || 0);
    const failureRate = Math.max(0, Number(upstream.failure_rate) || 0);
    if (samples === 0) return { text: "暂无数据", kind: "neutral" };
    if (successes === 0) return { text: "不可用", kind: "bad" };
    if (failureRate > 0 || successes < samples) return { text: "有失败", kind: "warn" };
    return { text: "正常", kind: "good" };
  }

  function start() {
    const state = {
      page: 1, pageSize: 50, search: "", controllers: new Map(),
      queryLoading: false, totalPages: null, trendData: null,
      refreshResults: new Map(), refreshPending: 0, queryRequestId: 0
    };
    const regions = Object.fromEntries(["trend", "upstreams", "rankings", "queries"].map((name) => [name, document.querySelector(`#${name}`)]));
    const previousButton = document.querySelector("#previous-page");
    const nextButton = document.querySelector("#next-page");

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
    function updateRefreshStatus() {
      const status = document.querySelector("#refresh-status");
      const dot = document.querySelector(".status-dot");
      if (state.refreshPending > 0) {
        status.textContent = "正在更新";
        dot.className = "status-dot is-loading";
        return;
      }
      const failed = [...state.refreshResults.values()].filter((result) => result === false).length;
      status.textContent = failed ? `${failed} 个区域更新失败` : "全部区域已更新";
      dot.className = failed ? "status-dot has-error" : "status-dot";
      if (!failed && state.refreshResults.size === 4) document.querySelector("#last-updated").textContent = `最后成功：${new Date().toLocaleTimeString()}`;
    }
    async function loadRegion(name, url, render) {
      state.controllers.get(name)?.abort();
      const controller = new AbortController();
      state.controllers.set(name, controller);
      state.refreshPending += 1;
      updateRefreshStatus();
      try {
        const response = await fetch(url, { signal: controller.signal });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = await response.json();
        if (state.controllers.get(name) !== controller) return false;
        await render(data);
        setError(name, "");
        state.refreshResults.set(name, true);
        return true;
      } catch (error) {
        if (error.name !== "AbortError") {
          setError(name, "更新失败，正在保留上次数据");
          state.refreshResults.set(name, false);
        }
        return false;
      } finally {
        state.refreshPending -= 1;
        if (state.controllers.get(name) === controller) state.controllers.delete(name);
        updateRefreshStatus();
      }
    }
    function renderTrend(data) {
      const buckets = Array.isArray(data.buckets) ? data.buckets : [];
      state.trendData = { buckets, granularity: data.granularity };
      window.DnsTrendChart.render(document.querySelector("#trend-chart"), buckets, data.granularity);
      const start = new Date(data.start);
      const end = new Date(data.end);
      const aggregation = data.granularity === "day" ? "按日聚合" : "按小时聚合";
      document.querySelector("#trend-range").textContent = `${aggregation} · ${start.toLocaleString()} 至 ${end.toLocaleString()}`;
      const totals = buckets.reduce((sum, bucket) => ({
        total: sum.total + window.DnsTrendChart.normalizeValue(bucket.total_queries),
        blocked: sum.blocked + window.DnsTrendChart.normalizeValue(bucket.blocked_queries),
        cache: sum.cache + window.DnsTrendChart.normalizeValue(bucket.cache_hits)
      }), { total: 0, blocked: 0, cache: 0 });
      document.querySelector("#trend-summary").textContent = `${aggregation}，共 ${window.DnsTrendChart.formatCount(totals.total)} 次查询，${window.DnsTrendChart.formatCount(totals.blocked)} 次屏蔽，${window.DnsTrendChart.formatCount(totals.cache)} 次缓存命中`;
    }
    function renderUpstreams(data) {
      const target = document.querySelector("#upstream-list");
      if (!Array.isArray(data) || data.length === 0) return replaceChildren(target, [element("p", "暂无上游数据", "empty")]);
      replaceChildren(target, data.map((upstream) => {
        const card = element("article", undefined, "upstream-card");
        const heading = element("div", undefined, "upstream-heading");
        const names = element("div");
        names.append(element("strong", upstream.name), element("span", upstream.group, "muted"));
        const status = upstreamStatus(upstream);
        heading.append(names, element("span", status.text, `badge badge-${status.kind}`));
        const metrics = element("dl", undefined, "metrics");
        const latency = Number(upstream.avg_latency_ms);
        [["平均延迟", Number.isFinite(latency) ? `${latency.toFixed(1)} ms` : "--"], ["样本", upstream.samples], ["失败率", `${((Number(upstream.failure_rate) || 0) * 100).toFixed(1)}%`]].forEach(([label, value]) => {
          const group = element("div"); group.append(element("dt", label), element("dd", value)); metrics.append(group);
        });
        card.append(heading, metrics); return card;
      }));
    }
    function emptyRow(columns, message) {
      const row = element("tr"); const cell = element("td", message, "empty"); cell.colSpan = columns; row.append(cell); return row;
    }
    function renderRankings(data) {
      const body = document.querySelector("#ranking-body");
      if (!Array.isArray(data) || data.length === 0) return replaceChildren(body, [emptyRow(4, "暂无域名排行")]);
      replaceChildren(body, data.map((record, index) => {
        const row = element("tr"); const domain = element("td");
        domain.append(element("span", String(index + 1).padStart(2, "0"), "rank"), element("span", record.domain));
        row.append(domain, element("td", record.total_queries), element("td", record.blocked_queries), element("td", record.cache_hits)); return row;
      }));
    }
    function addBadge(target, text, kind) { target.append(element("span", text, `badge ${kind}`)); }
    function renderQueries(data) {
      const body = document.querySelector("#query-body"); const records = Array.isArray(data.records) ? data.records : [];
      if (!records.length) replaceChildren(body, [emptyRow(5, state.search ? "没有匹配的查询记录" : "暂无查询记录")]);
      else replaceChildren(body, records.map((record) => {
        const row = element("tr"); const date = new Date(record.timestamp); const domain = element("td");
        domain.append(element("strong", record.domain), element("span", record.query_type, "muted block"));
        const ips = element("td"); const responseIps = Array.isArray(record.response_ips) ? record.response_ips : [];
        if (!responseIps.length) ips.append(element("span", "--", "muted")); else responseIps.forEach((ip) => ips.append(element("code", ip)));
        const result = element("td", undefined, "result-cell"); addBadge(result, record.response_code || "UNKNOWN", "badge-neutral");
        if (record.blocked) addBadge(result, "已屏蔽", "badge-blocked"); if (record.cache_hit) addBadge(result, "缓存", "badge-cache");
        row.append(element("td", Number.isNaN(date.getTime()) ? "--" : date.toLocaleString()), domain, ips, element("td", `${record.duration_ms} ms`), result); return row;
      }));
      document.querySelector("#page-status").textContent = `第 ${state.page} / ${state.totalPages} 页 · ${Math.max(0, Number(data.total) || 0)} 条`;
      updatePagination();
    }
    async function loadQueries(corrected) {
      const requestId = ++state.queryRequestId;
      setQueryLoading(true);
      const requestedPage = state.page;
      const query = `page=${requestedPage}&page_size=${state.pageSize}&search=${encodeURIComponent(state.search)}`;
      let retry = false;
      const success = await loadRegion("queries", `/api/dashboard/queries?${query}`, (data) => {
        const decision = paginationDecision(requestedPage, state.pageSize, data.total, corrected);
        state.totalPages = decision.totalPages;
        state.page = decision.page;
        retry = decision.retry;
        if (!retry) renderQueries(data);
      });
      if (success && retry) return loadQueries(true);
      if (mayFinishQuery(requestId, state.queryRequestId)) setQueryLoading(false);
      return success;
    }
    function refreshAll() {
      state.refreshResults.clear();
      return Promise.allSettled([
        loadRegion("trend", "/api/dashboard/trend", renderTrend), loadRegion("upstreams", "/api/dashboard/upstreams", renderUpstreams),
        loadRegion("rankings", "/api/dashboard/rankings", renderRankings), loadQueries(false)
      ]);
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
    window.addEventListener("resize", () => { window.clearTimeout(resizeTimer); resizeTimer = window.setTimeout(() => { if (state.trendData) window.DnsTrendChart.render(document.querySelector("#trend-chart"), state.trendData.buckets, state.trendData.granularity); }, 120); });
    updatePagination(); refreshAll(); window.setInterval(refreshAll, 5000);
  }

  return { start, paginationDecision, paginationControls, mayFinishQuery, searchDecision, upstreamStatus };
}));
