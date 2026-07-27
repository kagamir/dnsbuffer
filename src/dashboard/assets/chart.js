/* dnsbuffer chart module v1.1.0 */
(function (root, factory) {
  "use strict";
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.DnsTrendChart = api;
}(typeof window !== "undefined" ? window : globalThis, function () {
  "use strict";

  const series = [
    { key: "total_queries", label: "全部查询", color: "#5eead4", dash: [] },
    { key: "blocked_queries", label: "已屏蔽", color: "#fb7185", dash: [7, 5] },
    { key: "cache_hits", label: "缓存命中", color: "#fbbf24", dash: [2, 4] }
  ];

  const chartStates = new WeakMap();
  const hoverBound = new WeakSet();

  function normalizeValue(value) {
    const number = Number(value);
    return Number.isFinite(number) && number >= 0 ? number : 0;
  }

  function formatCount(value) {
    const number = normalizeValue(value);
    if (number >= 1000000) return `${Number((number / 1000000).toFixed(1))}M`;
    if (number >= 1000) return `${Number((number / 1000).toFixed(1))}K`;
    return String(Math.round(number));
  }

  function formatBucketLabel(timestamp, granularity, locale) {
    const date = new Date(timestamp);
    if (Number.isNaN(date.getTime())) return "--";
    if (granularity === "day") return date.toLocaleDateString(locale, { month: "short", day: "numeric" });
    return date.toLocaleString(locale, { month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false });
  }

  /* 将画布内 x 坐标映射到最近的数据桶下标；落在绘图区（含 8px 容差）外返回 null。 */
  function hoverIndex(x, left, plotWidth, count) {
    if (!Number.isFinite(x) || !Number.isFinite(left) || !Number.isFinite(plotWidth)) return null;
    if (plotWidth <= 0 || !Number.isInteger(count) || count <= 0) return null;
    if (x < left - 8 || x > left + plotWidth + 8) return null;
    if (count === 1) return 0;
    const ratio = Math.min(1, Math.max(0, (x - left) / plotWidth));
    return Math.round(ratio * (count - 1));
  }

  /* 悬浮提示的三行内容：与图例同色同序。 */
  function tooltipRows(bucket) {
    return series.map(({ key, label, color }) => ({
      label,
      color,
      value: normalizeValue(bucket ? bucket[key] : 0)
    }));
  }

  /* 观测曲线的 x 轴上限：以观测点范围为主，向上取整到易读刻度；
     配置容量在观测最大值 10 倍以内时也纳入范围，否则不为它撑大坐标轴
     （渲染时会在角落以文字标注配置值）。 */
  function cacheCurveAxisMax(points, maxEntries) {
    const observed = Math.max(
      1,
      ...(Array.isArray(points) ? points.map((point) => normalizeValue(point.size)) : [])
    );
    const configured = normalizeValue(maxEntries);
    const target = configured >= 1 && configured <= observed * 10
      ? Math.max(observed, configured)
      : observed;
    const magnitude = Math.pow(10, Math.floor(Math.log10(target)));
    return Math.ceil(target / magnitude) * magnitude;
  }

  function setupCanvas(canvas, minHeight) {
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(1, Math.floor(rect.width));
    const height = Math.max(minHeight, Math.floor(rect.height));
    const dpr = Math.max(1, Number.isFinite(window.devicePixelRatio) ? window.devicePixelRatio : 1);
    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(height * dpr);
    const context = canvas.getContext("2d");
    context.setTransform(dpr, 0, 0, dpr, 0, 0);
    context.clearRect(0, 0, width, height);
    context.font = "12px ui-monospace, SFMono-Regular, Consolas, monospace";
    return { context, width, height };
  }

  function drawEmpty(context, width, height, message) {
    context.fillStyle = "#8793a8";
    context.textAlign = "center";
    context.textBaseline = "alphabetic";
    context.fillText(message, width / 2, height / 2);
  }

  function drawTrend(canvas, state, hover) {
    const { context, width, height } = setupCanvas(canvas, 160);
    const buckets = state.buckets;
    if (!Array.isArray(buckets) || buckets.length === 0) {
      state.layout = null;
      return drawEmpty(context, width, height, "暂无查询数据");
    }

    const maximum = Math.max(1, ...buckets.map((bucket) => normalizeValue(bucket.total_queries)));
    const topTick = maximum * 1.08;
    const widestTick = context.measureText(formatCount(topTick)).width;
    const inset = { top: 18, right: 10, bottom: 38, left: Math.ceil(widestTick) + 15 };
    const plotWidth = Math.max(1, width - inset.left - inset.right);
    const plotHeight = Math.max(1, height - inset.top - inset.bottom);
    state.layout = { left: inset.left, top: inset.top, plotWidth, plotHeight };

    context.lineWidth = 1;
    context.textAlign = "right";
    context.textBaseline = "middle";
    context.setLineDash([]);
    for (let line = 0; line <= 4; line += 1) {
      const y = inset.top + (plotHeight * line / 4);
      context.strokeStyle = "#253047";
      context.beginPath();
      context.moveTo(inset.left, y);
      context.lineTo(width - inset.right, y);
      context.stroke();
      context.fillStyle = "#8793a8";
      context.fillText(formatCount(topTick * (4 - line) / 4), inset.left - 7, y);
    }

    const xAt = (index) => inset.left + (buckets.length === 1 ? plotWidth / 2 : plotWidth * index / (buckets.length - 1));
    const yAt = (value) => inset.top + plotHeight - (normalizeValue(value) / topTick * plotHeight);
    context.textBaseline = "top";
    context.fillStyle = "#8793a8";
    [...new Set([0, Math.floor((buckets.length - 1) / 2), buckets.length - 1])].forEach((index) => {
      context.textAlign = index === 0 ? "left" : index === buckets.length - 1 ? "right" : "center";
      context.fillText(formatBucketLabel(buckets[index].timestamp, state.granularity), xAt(index), height - 26);
    });

    series.forEach(({ key, color, dash }) => {
      context.strokeStyle = color;
      context.lineWidth = 2;
      context.lineJoin = "round";
      context.setLineDash(dash);
      context.beginPath();
      buckets.forEach((bucket, index) => {
        const x = xAt(index);
        const y = yAt(bucket[key]);
        if (index === 0) context.moveTo(x, y);
        else context.lineTo(x, y);
      });
      context.stroke();
    });
    context.setLineDash([]);

    if (hover != null && buckets[hover]) drawTrendHover(context, state, hover, width, xAt, yAt);
  }

  function drawTrendHover(context, state, hover, width, xAt, yAt) {
    const bucket = state.buckets[hover];
    const layout = state.layout;
    const x = xAt(hover);

    context.setLineDash([3, 4]);
    context.strokeStyle = "#8793a8";
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(x, layout.top);
    context.lineTo(x, layout.top + layout.plotHeight);
    context.stroke();
    context.setLineDash([]);
    series.forEach(({ key, color }) => {
      context.fillStyle = color;
      context.beginPath();
      context.arc(x, yAt(bucket[key]), 3.2, 0, Math.PI * 2);
      context.fill();
    });

    const title = formatBucketLabel(bucket.timestamp, state.granularity);
    const rows = tooltipRows(bucket);
    const lineHeight = 17;
    const paddingX = 10;
    const paddingY = 8;
    const swatch = 9;
    const rowTexts = rows.map((row) => `${row.label} ${formatCount(row.value)}`);
    const textWidth = Math.max(
      context.measureText(title).width,
      ...rowTexts.map((text) => context.measureText(text).width + swatch + 6)
    );
    const boxWidth = Math.ceil(textWidth) + paddingX * 2;
    const boxHeight = paddingY * 2 + lineHeight * (rows.length + 1);
    let boxX = x + 12;
    if (boxX + boxWidth > width - 4) boxX = x - 12 - boxWidth;
    if (boxX < 4) boxX = 4;
    const boxY = layout.top + 4;

    context.fillStyle = "rgba(8, 11, 18, .95)";
    context.fillRect(boxX, boxY, boxWidth, boxHeight);
    context.strokeStyle = "#253047";
    context.strokeRect(boxX + 0.5, boxY + 0.5, boxWidth - 1, boxHeight - 1);
    context.textAlign = "left";
    context.textBaseline = "middle";
    context.fillStyle = "#8793a8";
    context.fillText(title, boxX + paddingX, boxY + paddingY + lineHeight / 2);
    rows.forEach((row, index) => {
      const lineY = boxY + paddingY + lineHeight * (index + 1) + lineHeight / 2;
      context.fillStyle = row.color;
      context.fillRect(boxX + paddingX, lineY - 1.5, swatch, 3);
      context.fillStyle = "#e8edf6";
      context.fillText(rowTexts[index], boxX + paddingX + swatch + 6, lineY);
    });
  }

  function bindHover(canvas) {
    if (typeof canvas.addEventListener !== "function" || hoverBound.has(canvas)) return;
    hoverBound.add(canvas);
    canvas.addEventListener("mousemove", (event) => {
      const state = chartStates.get(canvas);
      if (!state || !state.layout) return;
      const rect = canvas.getBoundingClientRect();
      const index = hoverIndex(event.clientX - rect.left, state.layout.left, state.layout.plotWidth, state.buckets.length);
      if (index !== state.hover) {
        state.hover = index;
        drawTrend(canvas, state, index);
      }
    });
    canvas.addEventListener("mouseleave", () => {
      const state = chartStates.get(canvas);
      if (!state || state.hover == null) return;
      state.hover = null;
      drawTrend(canvas, state, null);
    });
  }

  function render(canvas, buckets, granularity) {
    const state = {
      buckets: Array.isArray(buckets) ? buckets : [],
      granularity,
      hover: null,
      layout: null
    };
    chartStates.set(canvas, state);
    drawTrend(canvas, state, null);
    bindHover(canvas);
  }

  /* 命中率-缓存大小曲线：每个点是一次观测 (当时缓存条数, 累计命中率)，
     按容量升序连成线；竖虚线标记配置容量，空心圆标记最近一次观测。 */
  function renderCacheCurve(canvas, data) {
    const { context, width, height } = setupCanvas(canvas, 160);
    const points = Array.isArray(data && data.points) ? data.points : [];
    if (points.length === 0) return drawEmpty(context, width, height, "正在收集观测数据...");

    const configured = normalizeValue(data.max_entries);
    const maxSize = cacheCurveAxisMax(points, configured);
    const inset = { top: 18, right: 12, bottom: 38, left: 46 };
    const plotWidth = Math.max(1, width - inset.left - inset.right);
    const plotHeight = Math.max(1, height - inset.top - inset.bottom);
    const xAt = (size) => inset.left + plotWidth * Math.min(1, normalizeValue(size) / maxSize);
    const yAt = (rate) => inset.top + plotHeight - Math.min(1, Math.max(0, Number(rate) || 0)) * plotHeight;

    context.lineWidth = 1;
    context.textAlign = "right";
    context.textBaseline = "middle";
    context.setLineDash([]);
    for (let line = 0; line <= 4; line += 1) {
      const y = inset.top + (plotHeight * line / 4);
      context.strokeStyle = "#253047";
      context.beginPath();
      context.moveTo(inset.left, y);
      context.lineTo(width - inset.right, y);
      context.stroke();
      context.fillStyle = "#8793a8";
      context.fillText(`${100 - line * 25}%`, inset.left - 7, y);
    }

    context.textBaseline = "top";
    context.fillStyle = "#8793a8";
    [0, 0.5, 1].forEach((fraction) => {
      context.textAlign = fraction === 0 ? "left" : fraction === 1 ? "right" : "center";
      context.fillText(formatCount(maxSize * fraction), inset.left + plotWidth * fraction, height - 26);
    });

    if (configured >= 1 && configured <= maxSize) {
      const configX = xAt(configured);
      context.setLineDash([4, 4]);
      context.strokeStyle = "#fbbf24";
      context.beginPath();
      context.moveTo(configX, inset.top);
      context.lineTo(configX, inset.top + plotHeight);
      context.stroke();
      context.setLineDash([]);
      context.fillStyle = "#fbbf24";
      context.textBaseline = "alphabetic";
      context.textAlign = configX > width - 70 ? "right" : "left";
      context.fillText(`配置 ${formatCount(configured)}`, configX + (configX > width - 70 ? -5 : 5), inset.top + 10);
    } else if (configured >= 1) {
      // 配置容量远超观测范围：不为它撑大坐标轴，只在右上角提示
      context.fillStyle = "#fbbf24";
      context.textBaseline = "alphabetic";
      context.textAlign = "right";
      context.fillText(`配置 ${formatCount(configured)} →`, width - inset.right, inset.top + 10);
    }

    context.strokeStyle = "#5eead4";
    context.lineWidth = 2;
    context.lineJoin = "round";
    context.beginPath();
    points.forEach((point, index) => {
      const x = xAt(point.size);
      const y = yAt(point.hit_rate);
      if (index === 0) context.moveTo(x, y);
      else context.lineTo(x, y);
    });
    context.stroke();
    context.fillStyle = "#5eead4";
    points.forEach((point) => {
      context.beginPath();
      context.arc(xAt(point.size), yAt(point.hit_rate), 2, 0, Math.PI * 2);
      context.fill();
    });

    const current = data.current;
    if (current && Number.isFinite(Number(current.size))) {
      context.beginPath();
      context.arc(xAt(current.size), yAt(current.hit_rate), 4.5, 0, Math.PI * 2);
      context.strokeStyle = "#e8edf6";
      context.lineWidth = 1.5;
      context.stroke();
    }
  }

  return {
    render,
    renderCacheCurve,
    normalizeValue,
    formatCount,
    formatBucketLabel,
    hoverIndex,
    tooltipRows,
    cacheCurveAxisMax,
    series
  };
}));
