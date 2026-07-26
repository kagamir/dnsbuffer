(function () {
  "use strict";

  const state = { page: 1, pageSize: 50, search: "", controllers: new Map() };
  const regions = {
    trend: document.querySelector("#trend"),
    upstreams: document.querySelector("#upstreams"),
    rankings: document.querySelector("#rankings"),
    queries: document.querySelector("#queries")
  };

  function element(tag, text, className) {
    const node = document.createElement(tag);
    if (text !== undefined) node.textContent = String(text);
    if (className) node.className = className;
    return node;
  }

  function replaceChildren(target, children) {
    target.replaceChildren(...children);
  }

  function setError(name, message) {
    const error = regions[name].querySelector(".panel-error");
    error.textContent = message || "";
    error.classList.toggle("is-visible", Boolean(message));
  }

  async function loadRegion(name, url, render) {
    state.controllers.get(name)?.abort();
    const controller = new AbortController();
    state.controllers.set(name, controller);
    try {
      const response = await fetch(url, { signal: controller.signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (state.controllers.get(name) !== controller) return;
      render(data);
      setError(name, "");
    } catch (error) {
      if (error.name !== "AbortError") setError(name, "更新失败，正在保留上次数据");
    } finally {
      if (state.controllers.get(name) === controller) state.controllers.delete(name);
    }
  }

  function renderTrend(data) {
    window.DnsTrendChart.render(document.querySelector("#trend-chart"), data.buckets);
  }

  function renderUpstreams(data) {
    const target = document.querySelector("#upstream-list");
    if (!Array.isArray(data) || data.length === 0) {
      replaceChildren(target, [element("p", "暂无上游数据", "empty")]);
      return;
    }
    replaceChildren(target, data.map((upstream) => {
      const card = element("article", undefined, "upstream-card");
      const heading = element("div", undefined, "upstream-heading");
      const names = element("div");
      names.append(element("strong", upstream.name), element("span", upstream.group, "muted"));
      const rate = Number(upstream.failure_rate) || 0;
      const health = element("span", rate === 0 ? "正常" : "有失败", rate === 0 ? "badge badge-good" : "badge badge-warn");
      heading.append(names, health);
      const metrics = element("dl", undefined, "metrics");
      [["平均延迟", upstream.avg_latency_ms == null ? "--" : `${Number(upstream.avg_latency_ms).toFixed(1)} ms`], ["样本", upstream.samples], ["失败率", `${(rate * 100).toFixed(1)}%`]].forEach(([label, value]) => {
        const group = element("div");
        group.append(element("dt", label), element("dd", value));
        metrics.append(group);
      });
      card.append(heading, metrics);
      return card;
    }));
  }

  function emptyRow(columns, message) {
    const row = element("tr");
    const cell = element("td", message, "empty");
    cell.colSpan = columns;
    row.append(cell);
    return row;
  }

  function renderRankings(data) {
    const body = document.querySelector("#ranking-body");
    if (!Array.isArray(data) || data.length === 0) {
      replaceChildren(body, [emptyRow(4, "暂无域名排行")]);
      return;
    }
    replaceChildren(body, data.map((record, index) => {
      const row = element("tr");
      const domain = element("td");
      domain.append(element("span", String(index + 1).padStart(2, "0"), "rank"), element("span", record.domain));
      row.append(domain, element("td", record.total_queries), element("td", record.blocked_queries), element("td", record.cache_hits));
      return row;
    }));
  }

  function addBadge(target, text, kind) {
    target.append(element("span", text, `badge ${kind}`));
  }

  function renderQueries(data) {
    const body = document.querySelector("#query-body");
    const records = Array.isArray(data.records) ? data.records : [];
    if (records.length === 0) {
      replaceChildren(body, [emptyRow(5, state.search ? "没有匹配的查询记录" : "暂无查询记录")]);
    } else {
      replaceChildren(body, records.map((record) => {
        const row = element("tr");
        const date = new Date(record.timestamp);
        const domain = element("td");
        domain.append(element("strong", record.domain), element("span", record.query_type, "muted block"));
        const ips = element("td");
        const responseIps = Array.isArray(record.response_ips) ? record.response_ips : [];
        if (responseIps.length === 0) ips.append(element("span", "--", "muted"));
        else responseIps.forEach((ip) => ips.append(element("code", ip)));
        const result = element("td", undefined, "result-cell");
        addBadge(result, record.response_code || "UNKNOWN", "badge-neutral");
        if (record.blocked) addBadge(result, "已屏蔽", "badge-blocked");
        if (record.cache_hit) addBadge(result, "缓存", "badge-cache");
        row.append(
          element("td", Number.isNaN(date.getTime()) ? "--" : date.toLocaleString()),
          domain,
          ips,
          element("td", `${record.duration_ms} ms`),
          result
        );
        return row;
      }));
    }

    const total = Math.max(0, Number(data.total) || 0);
    const totalPages = Math.max(1, Math.ceil(total / state.pageSize));
    document.querySelector("#page-status").textContent = `第 ${state.page} / ${totalPages} 页 · ${total} 条`;
    document.querySelector("#previous-page").disabled = state.page <= 1;
    document.querySelector("#next-page").disabled = state.page >= totalPages;
  }

  function loadQueries() {
    const query = `page=${state.page}&page_size=${state.pageSize}&search=${encodeURIComponent(state.search)}`;
    return loadRegion("queries", `/api/dashboard/queries?${query}`, renderQueries);
  }

  function refreshAll() {
    return Promise.allSettled([
      loadRegion("trend", "/api/dashboard/trend", renderTrend),
      loadRegion("upstreams", "/api/dashboard/upstreams", renderUpstreams),
      loadRegion("rankings", "/api/dashboard/rankings", renderRankings),
      loadQueries()
    ]);
  }

  let searchTimer;
  document.querySelector("#query-search").addEventListener("input", (event) => {
    window.clearTimeout(searchTimer);
    const value = event.target.value;
    searchTimer = window.setTimeout(() => {
      state.search = value.trim();
      state.page = 1;
      loadQueries();
    }, 300);
  });

  document.querySelector("#previous-page").addEventListener("click", () => {
    if (state.page > 1) {
      state.page -= 1;
      loadQueries();
    }
  });
  document.querySelector("#next-page").addEventListener("click", () => {
    state.page += 1;
    loadQueries();
  });

  window.addEventListener("resize", () => loadRegion("trend", "/api/dashboard/trend", renderTrend));
  refreshAll();
  window.setInterval(refreshAll, 5000);
}());
