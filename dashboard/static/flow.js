const $ = (id) => document.getElementById(id);

function renderFlowItem(item) {
  const el = document.createElement('div');
  el.className = 'flow-item';
  el.innerHTML = `
    <div class="flow-head">
      <span class="pill">${item.kafka_topic || item.sourcetype || item.stage || 'event'}</span>
      <strong>${item.event_type || item.stage || 'live event'}</strong>
    </div>
    <div class="subtle">${item.transaction_id || ''} · ${item.service || item.host || ''} · ${item.timestamp || item._time || ''}</div>
    <div class="flow-meta">${item.status || ''} ${item.failure_code ? `| ${item.failure_code}` : ''}</div>
  `;
  return el;
}

async function fetchJSON(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
  return res.json();
}

async function refreshFlow() {
  const data = await fetchJSON('/api/flow');
  const out = $('flow-items');
  out.innerHTML = '';
  data.items.forEach((item) => out.appendChild(renderFlowItem(item)));
  const detectionCount = data.items.filter((item) => item.kafka_topic === 'recovery.events' || item.sourcetype === 'revtrace:detection').length;
  $('flow-count').textContent = `${data.items.length} recent events, ${detectionCount} detections`;
}

refreshFlow();
setInterval(refreshFlow, 3000);
