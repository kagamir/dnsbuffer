/* dnsbuffer chart module v1.0.0 */
(function (root, factory) {
  "use strict";
  const api = factory();
  if (typeof module === "object" && module.exports) module.exports = api;
  if (root) root.DnsTrendChart = api;
}(typeof window !== "undefined" ? window : globalThis, function () {
  "use strict";

  const series = [
    { key: "total_queries", color: "#5eead4", dash: [] },
    { key: "blocked_queries", color: "#fb7185", dash: [7, 5] },
    { key: "cache_hits", color: "#fbbf24", dash: [2, 4] }
  ];

  function parseCount(value) {
    if (typeof value === "bigint") return value >= 0n ? value : 0n;
    if (typeof value !== "string" || !/^\d+$/.test(value)) return 0n;
    return BigInt(value);
  }

  function formatCount(value) {
    const count = parseCount(value);
    if (count < 1000n) return count.toString();
    const unit = count >= 1000000n ? 1000000n : 1000n;
    const suffix = unit === 1000000n ? "M" : "K";
    const tenths = count * 10n / unit;
    return `${tenths / 10n}${tenths % 10n === 0n ? "" : `.${tenths % 10n}`}${suffix}`;
  }

  function exactCount(value) {
    return parseCount(value).toString();
  }

  function formatBucketLabel(timestamp, granularity, locale) {
    const date = new Date(timestamp);
    if (Number.isNaN(date.getTime())) return "--";
    if (granularity === "day") return date.toLocaleDateString(locale, { month: "short", day: "numeric" });
    return date.toLocaleString(locale, { month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit", hour12: false });
  }

  function render(canvas, buckets, granularity) {
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(1, Math.floor(rect.width));
    const height = Math.max(160, Math.floor(rect.height));
    const dpr = Math.max(1, Number.isFinite(window.devicePixelRatio) ? window.devicePixelRatio : 1);
    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(height * dpr);

    const context = canvas.getContext("2d");
    context.setTransform(dpr, 0, 0, dpr, 0, 0);
    context.clearRect(0, 0, width, height);
    context.font = "12px ui-monospace, SFMono-Regular, Consolas, monospace";

    if (!Array.isArray(buckets) || buckets.length === 0) {
      context.fillStyle = "#8793a8";
      context.textAlign = "center";
      context.fillText("暂无查询数据", width / 2, height / 2);
      return;
    }

    const maximum = buckets.reduce((max, bucket) => {
      const count = parseCount(bucket.total_queries);
      return count > max ? count : max;
    }, 1n);
    const topTick = maximum * 108n / 100n || 1n;
    const widestTick = context.measureText(formatCount(topTick)).width;
    const inset = { top: 18, right: 10, bottom: 38, left: Math.ceil(widestTick) + 15 };
    const plotWidth = Math.max(1, width - inset.left - inset.right);
    const plotHeight = Math.max(1, height - inset.top - inset.bottom);

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
      context.fillText(formatCount(topTick * BigInt(4 - line) / 4n), inset.left - 7, y);
    }

    const xAt = (index) => inset.left + (buckets.length === 1 ? plotWidth / 2 : plotWidth * index / (buckets.length - 1));
    const yAt = (value) => {
      const scaled = Number(parseCount(value) * 1000000n / topTick) / 1000000;
      return inset.top + plotHeight - scaled * plotHeight;
    };
    context.textBaseline = "top";
    context.fillStyle = "#8793a8";
    [...new Set([0, Math.floor((buckets.length - 1) / 2), buckets.length - 1])].forEach((index) => {
      context.textAlign = index === 0 ? "left" : index === buckets.length - 1 ? "right" : "center";
      context.fillText(formatBucketLabel(buckets[index].timestamp, granularity), xAt(index), height - 26);
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
  }

  return { render, parseCount, formatCount, exactCount, formatBucketLabel, series };
}));
