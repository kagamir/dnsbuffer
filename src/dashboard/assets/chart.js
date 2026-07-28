/* dnsbuffer chart module v1.2.0 */
(function (root, factory) {
  "use strict";
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.DnsTrendChart = api;
}(typeof window !== "undefined" ? window : globalThis, function () {
  "use strict";

  /* Static series colors double as the light-pattern contract and the dark-theme
     fallback; the actual stroke/fill colors are resolved per-theme at draw time
     (see readColors / seriesColor). Keeping these stable preserves tooltipRows'
     data contract for the unit tests. */
  const series = [
    { key: "total_queries", label: "All queries", color: "#5eead4", dash: [] },
    { key: "blocked_queries", label: "Blocked", color: "#fb7185", dash: [7, 5] },
    { key: "cache_hits", label: "Cache hits", color: "#fbbf24", dash: [2, 4] }
  ];

  const chartStates = new WeakMap();
  const hoverBound = new WeakSet();

  /* Theme colors read from CSS custom properties so charts follow light/dark.
     Falls back to the dark palette when there is no DOM (e.g. under node:test). */
  function readColors() {
    const fallback = {
      total: "#5eead4", blocked: "#fb7185", cache: "#fbbf24",
      grid: "#253047", axis: "#8793a8", fg: "#e8edf6", neutral: "#93c5fd",
      tooltipBg: "#0c1119", tooltipBorder: "#253047"
    };
    if (typeof document === "undefined" || typeof getComputedStyle !== "function") return fallback;
    const style = getComputedStyle(document.documentElement);
    const read = (name, fb) => {
      const value = style.getPropertyValue(name).trim();
      return value || fb;
    };
    return {
      total: read("--chart-total", fallback.total),
      blocked: read("--chart-blocked", fallback.blocked),
      cache: read("--chart-cache", fallback.cache),
      grid: read("--chart-grid", fallback.grid),
      axis: read("--chart-axis", fallback.axis),
      fg: read("--chart-fg", fallback.fg),
      neutral: read("--chart-neutral", fallback.neutral),
      tooltipBg: read("--chart-tooltip-bg", fallback.tooltipBg),
      tooltipBorder: read("--chart-tooltip-border", fallback.tooltipBorder)
    };
  }

  function seriesColor(key, colors) {
    if (key === "total_queries") return colors.total;
    if (key === "blocked_queries") return colors.blocked;
    return colors.cache;
  }

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

  /* Maps an x coordinate within the canvas to the nearest data bucket index; returns null when it falls outside the plot area (with an 8px tolerance). */
  function hoverIndex(x, left, plotWidth, count) {
    if (!Number.isFinite(x) || !Number.isFinite(left) || !Number.isFinite(plotWidth)) return null;
    if (plotWidth <= 0 || !Number.isInteger(count) || count <= 0) return null;
    if (x < left - 8 || x > left + plotWidth + 8) return null;
    if (count === 1) return 0;
    const ratio = Math.min(1, Math.max(0, (x - left) / plotWidth));
    return Math.round(ratio * (count - 1));
  }

  /* The three lines of the hover tooltip: same colors and order as the legend. */
  function tooltipRows(bucket) {
    return series.map(({ key, label, color }) => ({
      label,
      color,
      value: normalizeValue(bucket ? bucket[key] : 0)
    }));
  }

  /* Upper bound for a count axis: driven mainly by the observed sizes, rounded up
     to a readable tick; the configured capacity is included only when it is within
     10x the observed maximum, otherwise the axis is not stretched for it. */
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

  /* Two-bar snapshot model for the cache panel: one bar for the current entry
     count (left count axis, scaled against the configured capacity), one for the
     current cumulative hit rate (right 0-100% axis). Pure so it can be unit tested. */
  function cacheBars(data) {
    const currentEntries = normalizeValue(data && data.current_entries);
    const maxEntries = normalizeValue(data && data.max_entries);
    const rate = data && data.current ? Number(data.current.hit_rate) : NaN;
    const hasRate = Number.isFinite(rate);
    const hitRate = hasRate ? Math.min(1, Math.max(0, rate)) : 0;
    const countAxisMax = cacheCurveAxisMax([{ size: currentEntries }], maxEntries);
    return {
      countAxisMax,
      hasRate,
      bars: [
        {
          key: "count",
          label: "Entries",
          value: currentEntries,
          display: formatCount(currentEntries),
          ratio: countAxisMax > 0 ? Math.min(1, currentEntries / countAxisMax) : 0
        },
        {
          key: "rate",
          label: "Hit rate",
          value: hitRate,
          display: hasRate ? `${(hitRate * 100).toFixed(1)}%` : "--",
          ratio: hitRate
        }
      ]
    };
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

  function drawEmpty(context, width, height, message, color) {
    context.fillStyle = color || "#8793a8";
    context.textAlign = "center";
    context.textBaseline = "middle";
    context.fillText(message, width / 2, height / 2);
  }

  /* Fills a rectangle with its two top corners rounded (bars grow up from a baseline). */
  function fillBar(context, x, y, barWidth, barHeight, radius) {
    if (barHeight <= 0) return;
    const r = Math.max(0, Math.min(radius, barWidth / 2, barHeight));
    context.beginPath();
    context.moveTo(x, y + barHeight);
    context.lineTo(x, y + r);
    context.arcTo(x, y, x + r, y, r);
    context.lineTo(x + barWidth - r, y);
    context.arcTo(x + barWidth, y, x + barWidth, y + r, r);
    context.lineTo(x + barWidth, y + barHeight);
    context.closePath();
    context.fill();
  }

  function drawTrend(canvas, state, hover) {
    const colors = readColors();
    const { context, width, height } = setupCanvas(canvas, 160);
    const buckets = state.buckets;
    if (!Array.isArray(buckets) || buckets.length === 0) {
      state.layout = null;
      return drawEmpty(context, width, height, "No query data", colors.axis);
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
      context.strokeStyle = colors.grid;
      context.beginPath();
      context.moveTo(inset.left, y);
      context.lineTo(width - inset.right, y);
      context.stroke();
      context.fillStyle = colors.axis;
      context.fillText(formatCount(topTick * (4 - line) / 4), inset.left - 7, y);
    }

    const xAt = (index) => inset.left + (buckets.length === 1 ? plotWidth / 2 : plotWidth * index / (buckets.length - 1));
    const yAt = (value) => inset.top + plotHeight - (normalizeValue(value) / topTick * plotHeight);
    context.textBaseline = "top";
    context.fillStyle = colors.axis;
    [...new Set([0, Math.floor((buckets.length - 1) / 2), buckets.length - 1])].forEach((index) => {
      context.textAlign = index === 0 ? "left" : index === buckets.length - 1 ? "right" : "center";
      context.fillText(formatBucketLabel(buckets[index].timestamp, state.granularity), xAt(index), height - 26);
    });

    series.forEach(({ key, dash }) => {
      context.strokeStyle = seriesColor(key, colors);
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

    if (hover != null && buckets[hover]) drawTrendHover(context, state, hover, width, xAt, yAt, colors);
  }

  function drawTrendHover(context, state, hover, width, xAt, yAt, colors) {
    const bucket = state.buckets[hover];
    const layout = state.layout;
    const x = xAt(hover);

    context.setLineDash([3, 4]);
    context.strokeStyle = colors.axis;
    context.lineWidth = 1;
    context.beginPath();
    context.moveTo(x, layout.top);
    context.lineTo(x, layout.top + layout.plotHeight);
    context.stroke();
    context.setLineDash([]);
    series.forEach(({ key }) => {
      context.fillStyle = seriesColor(key, colors);
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

    context.fillStyle = colors.tooltipBg;
    context.fillRect(boxX, boxY, boxWidth, boxHeight);
    context.strokeStyle = colors.tooltipBorder;
    context.strokeRect(boxX + 0.5, boxY + 0.5, boxWidth - 1, boxHeight - 1);
    context.textAlign = "left";
    context.textBaseline = "middle";
    context.fillStyle = colors.axis;
    context.fillText(title, boxX + paddingX, boxY + paddingY + lineHeight / 2);
    rows.forEach((row, index) => {
      const lineY = boxY + paddingY + lineHeight * (index + 1) + lineHeight / 2;
      context.fillStyle = seriesColor(series[index].key, colors);
      context.fillRect(boxX + paddingX, lineY - 1.5, swatch, 3);
      context.fillStyle = colors.fg;
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

  /* Cache panel: two narrow bars with independent axes — current entry count on
     the left count axis, current cumulative hit rate on the right 0-100% axis. */
  function renderCacheBars(canvas, data) {
    const colors = readColors();
    const { context, width, height } = setupCanvas(canvas, 160);
    const model = cacheBars(data || {});
    if (model.bars[0].value === 0 && !model.hasRate) {
      return drawEmpty(context, width, height, "Collecting observations...", colors.axis);
    }

    const inset = { top: 24, right: 50, bottom: 34, left: 50 };
    const plotWidth = Math.max(1, width - inset.left - inset.right);
    const plotHeight = Math.max(1, height - inset.top - inset.bottom);
    const baseline = inset.top + plotHeight;
    const axisColors = [colors.total, colors.cache];

    // Horizontal gridlines with a count axis on the left and a percent axis on the right.
    context.lineWidth = 1;
    context.textBaseline = "middle";
    for (let line = 0; line <= 4; line += 1) {
      const y = inset.top + (plotHeight * line / 4);
      context.strokeStyle = colors.grid;
      context.beginPath();
      context.moveTo(inset.left, y);
      context.lineTo(width - inset.right, y);
      context.stroke();
      context.fillStyle = axisColors[0];
      context.textAlign = "right";
      context.fillText(formatCount(model.countAxisMax * (4 - line) / 4), inset.left - 8, y);
      context.fillStyle = axisColors[1];
      context.textAlign = "left";
      context.fillText(`${100 - line * 25}%`, width - inset.right + 8, y);
    }

    const barWidth = Math.min(48, Math.max(24, plotWidth / 4));
    const centers = [inset.left + plotWidth * 0.28, inset.left + plotWidth * 0.72];
    model.bars.forEach((bar, index) => {
      const barHeight = Math.max(0, Math.min(1, bar.ratio)) * plotHeight;
      const x = centers[index] - barWidth / 2;
      const y = baseline - barHeight;
      context.fillStyle = axisColors[index];
      fillBar(context, x, y, barWidth, barHeight, 4);

      context.fillStyle = colors.fg;
      context.textAlign = "center";
      context.textBaseline = "alphabetic";
      context.fillText(bar.display, centers[index], Math.max(inset.top + 10, y - 7));

      context.fillStyle = colors.axis;
      context.textBaseline = "top";
      context.fillText(bar.label, centers[index], baseline + 8);
    });
  }

  return {
    render,
    renderCacheBars,
    normalizeValue,
    formatCount,
    formatBucketLabel,
    hoverIndex,
    tooltipRows,
    cacheCurveAxisMax,
    cacheBars,
    seriesColor,
    readColors,
    series
  };
}));
