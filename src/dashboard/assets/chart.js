/* dnsbuffer chart module v1.0.0 */
(function () {
  "use strict";

  const series = [
    { key: "total_queries", color: "#5eead4" },
    { key: "blocked_queries", color: "#fb7185" },
    { key: "cache_hits", color: "#fbbf24" }
  ];

  function render(canvas, buckets) {
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(300, Math.floor(rect.width));
    const height = Math.max(210, Math.floor(rect.height));
    const dpr = Math.max(1, window.devicePixelRatio || 1);
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

    const inset = { top: 18, right: 14, bottom: 34, left: 42 };
    const plotWidth = width - inset.left - inset.right;
    const plotHeight = height - inset.top - inset.bottom;
    const maximum = Math.max(1, ...buckets.map((bucket) => Number(bucket.total_queries) || 0));

    context.lineWidth = 1;
    context.textAlign = "right";
    context.textBaseline = "middle";
    for (let line = 0; line <= 4; line += 1) {
      const y = inset.top + (plotHeight * line / 4);
      context.strokeStyle = "#253047";
      context.beginPath();
      context.moveTo(inset.left, y);
      context.lineTo(width - inset.right, y);
      context.stroke();
      context.fillStyle = "#8793a8";
      context.fillText(String(Math.round(maximum * (4 - line) / 4)), inset.left - 8, y);
    }

    const xAt = (index) => inset.left + (buckets.length === 1 ? plotWidth / 2 : plotWidth * index / (buckets.length - 1));
    const yAt = (value) => inset.top + plotHeight - (Math.max(0, Number(value) || 0) / maximum * plotHeight);

    context.textBaseline = "top";
    context.fillStyle = "#8793a8";
    const labels = [...new Set([0, Math.floor((buckets.length - 1) / 2), buckets.length - 1])];
    labels.forEach((index) => {
      const date = new Date(buckets[index].timestamp);
      context.textAlign = index === 0 ? "left" : index === buckets.length - 1 ? "right" : "center";
      context.fillText(Number.isNaN(date.getTime()) ? "--" : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }), xAt(index), height - 22);
    });

    series.forEach(({ key, color }) => {
      context.strokeStyle = color;
      context.lineWidth = 2;
      context.lineJoin = "round";
      context.beginPath();
      buckets.forEach((bucket, index) => {
        const x = xAt(index);
        const y = yAt(bucket[key]);
        if (index === 0) context.moveTo(x, y);
        else context.lineTo(x, y);
      });
      context.stroke();
    });
  }

  window.DnsTrendChart = { render };
}());
