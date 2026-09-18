// Real-Time Dashboard Client Logic

const feedOrders = document.getElementById("feedOrders");
const feedRetries = document.getElementById("feedRetries");
const feedDlq = document.getElementById("feedDlq");

const valAvg = document.getElementById("valAvg");
const valCount = document.getElementById("valCount");
const valLastPrice = document.getElementById("valLastPrice");
const valLastItem = document.getElementById("valLastItem");
const valDlqCount = document.getElementById("valDlqCount");

const countOrders = document.getElementById("countOrders");
const countRetries = document.getElementById("countRetries");
const countDlq = document.getElementById("countDlq");

const toggleStreamBtn = document.getElementById("toggleStreamBtn");
const sendNormalBtn = document.getElementById("sendNormalBtn");
const sendTransientBtn = document.getElementById("sendTransientBtn");
const sendBadPriceBtn = document.getElementById("sendBadPriceBtn");
const sendCorruptBtn = document.getElementById("sendCorruptBtn");
const clearFeedsBtn = document.getElementById("clearFeedsBtn");

const nodeProducer = document.getElementById("nodeProducer");
const nodeConsumer = document.getElementById("nodeConsumer");
const nodeAggregator = document.getElementById("nodeAggregator");
const nodeDlq = document.getElementById("nodeDlq");

// Canvas chart data
const canvas = document.getElementById("priceChart");
const ctx = canvas.getContext("2d");
const chartData = [];
const MAX_CHART_POINTS = 40;

let isStreaming = false;
let retryCountTotal = 0;
let dlqCountTotal = 0;

function pulseNode(node, duration = 600) {
  node.classList.add("active");
  setTimeout(() => node.classList.remove("active"), duration);
}

function updateChart(price, avg) {
  chartData.push({ price, avg });
  if (chartData.length > MAX_CHART_POINTS) {
    chartData.shift();
  }
  drawChart();
}

function drawChart() {
  const w = canvas.width;
  const h = canvas.height;
  ctx.clearRect(0, 0, w, h);

  if (chartData.length < 2) return;

  // Find price max & min
  let maxVal = 500;
  chartData.forEach((d) => {
    if (d.price > maxVal) maxVal = d.price;
    if (d.avg > maxVal) maxVal = d.avg;
  });
  maxVal = Math.ceil(maxVal * 1.1);

  const stepX = w / (MAX_CHART_POINTS - 1);
  const paddingY = 20;
  const graphH = h - paddingY * 2;

  // Grid lines
  ctx.strokeStyle = "#e2e8f0";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(0, h / 2);
  ctx.lineTo(w, h / 2);
  ctx.moveTo(0, h - 1);
  ctx.lineTo(w, h - 1);
  ctx.stroke();

  // Draw Price line (Light Indigo)
  ctx.strokeStyle = "#a5b4fc";
  ctx.lineWidth = 1.5;
  ctx.beginPath();
  chartData.forEach((d, i) => {
    const x = i * stepX;
    const y = h - paddingY - (d.price / maxVal) * graphH;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();

  // Draw Running Average line (Bold Indigo)
  ctx.strokeStyle = "#4f46e5";
  ctx.lineWidth = 3;
  ctx.beginPath();
  chartData.forEach((d, i) => {
    const x = i * stepX;
    const y = h - paddingY - (d.avg / maxVal) * graphH;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();

  // Draw last average point dot
  const lastIdx = chartData.length - 1;
  const lastX = lastIdx * stepX;
  const lastY = h - paddingY - (chartData[lastIdx].avg / maxVal) * graphH;
  ctx.fillStyle = "#4f46e5";
  ctx.beginPath();
  ctx.arc(lastX, lastY, 5, 0, Math.PI * 2);
  ctx.fill();
}

// Remove empty placeholder
function removeEmpty(parent) {
  const empty = parent.querySelector(".empty-state");
  if (empty) empty.remove();
}

// Connect to Server-Sent Events
function connectEvents() {
  const source = new EventSource("/api/stream");

  source.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);

      if (data.type === "init") {
        valAvg.textContent = `$${data.average.toFixed(2)}`;
        valCount.textContent = data.count;
        valDlqCount.textContent = data.dlqCount;
        countDlq.textContent = data.dlqCount;
        dlqCountTotal = data.dlqCount;
        setStreamState(data.isStreaming);
      } else if (data.type === "order") {
        removeEmpty(feedOrders);
        pulseNode(nodeConsumer);
        pulseNode(nodeAggregator);

        // Update metrics
        valAvg.textContent = `$${data.average.toFixed(2)}`;
        valCount.textContent = data.count;
        countOrders.textContent = data.count;
        valLastPrice.textContent = `$${data.order.price.toFixed(2)}`;
        valLastItem.textContent = `${data.order.product} (ID: ${data.order.orderId})`;

        // Append order item
        const item = document.createElement("div");
        item.className = "feed-item valid";
        item.innerHTML = `
          <div class="feed-top">
            <span class="item-id">#${data.order.orderId} · ${data.order.product}</span>
            <span class="item-price">$${data.order.price.toFixed(2)}</span>
          </div>
          <div class="item-desc">
            <span>Avg: $${data.average.toFixed(2)} (n=${data.count})</span>
            <span class="item-badge badge-tag-success">O(1) Incremental</span>
          </div>
        `;
        feedOrders.prepend(item);
        if (feedOrders.children.length > 50) {
          feedOrders.removeChild(feedOrders.lastChild);
        }

        updateChart(data.order.price, data.average);
      } else if (data.type === "retry") {
        removeEmpty(feedRetries);
        pulseNode(nodeConsumer);
        retryCountTotal++;
        countRetries.textContent = retryCountTotal;

        const item = document.createElement("div");
        item.className = "feed-item retry";
        const isRecovered = data.status === "recovered";
        item.innerHTML = `
          <div class="feed-top">
            <span class="item-id">#${data.orderId}</span>
            <span class="item-badge ${isRecovered ? "badge-tag-success" : "badge-tag-warning"}">
              ${isRecovered ? "Recovered (Att 3)" : `Attempt ${data.attempt}/3`}
            </span>
          </div>
          <div class="item-desc">
            <span>${data.message}</span>
            <span>${data.delaySec ? `${data.delaySec}s delay` : ""}</span>
          </div>
        `;
        feedRetries.prepend(item);
        if (feedRetries.children.length > 30) {
          feedRetries.removeChild(feedRetries.lastChild);
        }
      } else if (data.type === "dlq") {
        removeEmpty(feedDlq);
        pulseNode(nodeDlq);
        dlqCountTotal++;
        valDlqCount.textContent = dlqCountTotal;
        countDlq.textContent = dlqCountTotal;

        const item = document.createElement("div");
        item.className = "feed-item dlq";
        item.innerHTML = `
          <div class="feed-top">
            <span class="item-id">Offset ${data.envelope.offset}</span>
            <span class="item-badge badge-tag-danger">Quarantined</span>
          </div>
          <div class="item-desc" style="color: #991b1b; font-weight: 500;">
            ${data.envelope.errorReason}
          </div>
          <div class="raw-box">${JSON.stringify(data.envelope.payload, null, 2)}</div>
        `;
        feedDlq.prepend(item);
        if (feedDlq.children.length > 30) {
          feedDlq.removeChild(feedDlq.lastChild);
        }
      } else if (data.type === "stream-status") {
        setStreamState(data.isStreaming);
      }
    } catch (err) {
      console.error("SSE parse error:", err);
    }
  };

  source.onerror = () => {
    document.getElementById("brokerBadge").classList.remove("online");
    setTimeout(connectEvents, 3000);
  };
}

function setStreamState(streaming) {
  isStreaming = streaming;
  if (isStreaming) {
    toggleStreamBtn.textContent = "⏹ Pause Stream";
    toggleStreamBtn.className = "btn btn-danger";
    pulseNode(nodeProducer, 999999);
  } else {
    toggleStreamBtn.textContent = "▶ Auto Stream";
    toggleStreamBtn.className = "btn btn-primary";
    nodeProducer.classList.remove("active");
  }
}

// Button controls
async function triggerApi(url) {
  try {
    const res = await fetch(url, { method: "POST" });
    pulseNode(nodeProducer);
    return await res.json();
  } catch (err) {
    console.error("Action error:", err);
  }
}

toggleStreamBtn.addEventListener("click", () => {
  triggerApi("/api/control/stream/toggle");
});

sendNormalBtn.addEventListener("click", () => {
  triggerApi("/api/produce/single?mode=normal");
});

sendTransientBtn.addEventListener("click", () => {
  triggerApi("/api/produce/single?mode=transient");
});

sendBadPriceBtn.addEventListener("click", () => {
  triggerApi("/api/produce/single?mode=bad-price");
});

sendCorruptBtn.addEventListener("click", () => {
  triggerApi("/api/produce/single?mode=corrupt");
});

clearFeedsBtn.addEventListener("click", () => {
  feedOrders.innerHTML = '<div class="empty-state">Waiting for orders...</div>';
  feedRetries.innerHTML = '<div class="empty-state">No transient errors logged</div>';
  feedDlq.innerHTML = '<div class="empty-state">DLQ empty (0 quarantined)</div>';
  countOrders.textContent = "0";
  countRetries.textContent = "0";
  countDlq.textContent = "0";
});

// Initial startup
connectEvents();
drawChart();
