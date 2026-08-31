const $ = (id) => document.getElementById(id);

function parseRecord(item) {
  if (item && typeof item._raw === 'string') {
    try {
      const parsed = JSON.parse(item._raw);
      return { ...item, ...parsed, metadata: parsed.metadata || item.metadata };
    } catch {
      return item;
    }
  }
  return item;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function isDetectionEvent(item) {
  return item.sourcetype === 'revtrace:detection' || item.stage === 'detection' || Boolean(item.metadata?.root_cause);
}

function renderTable(items, kind) {
  const normalized = items.map(parseRecord);
  if (!normalized.length) {
    return kind === 'ingestion'
      ? '<div class="empty-state">No ingestion records in Splunk yet. Ingestion events live in Kafka topics unless you bridge them into the index.</div>'
      : '<div class="empty-state">No live records yet.</div>';
  }
  const rows = normalized.map((item) => {
    const status = String(item.status || '').toLowerCase();
    const anomaly = Boolean(item.metadata?.signals?.degradation?.anomaly || item.metadata?.signals?.failure || item.metadata?.root_cause);
    const rowClass = kind === 'detection' && (status === 'failure' || anomaly) ? 'failure' : '';
    return `
      <tr class="${rowClass}" data-record="${escapeHtml(JSON.stringify(item))}">
        <td>${escapeHtml(item._time || item.timestamp || '')}</td>
        <td>${escapeHtml(item.stage || item.sourcetype || '')}</td>
        <td>${escapeHtml(item.event_type || '')}</td>
        <td>${escapeHtml(item.status || '')}</td>
        <td>${escapeHtml(item.transaction_id || '')}</td>
        <td>${escapeHtml(item.customer_id || '')}</td>
        <td>${escapeHtml(item.failure_code || item.metadata?.root_cause?.failure_code || item.metadata?.root_cause?.stage || '')}</td>
      </tr>
    `;
  }).join('');

  return `
    <div class="table-wrap">
      <table class="records-table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Stage</th>
            <th>Event</th>
            <th>Status</th>
            <th>Transaction</th>
            <th>Customer</th>
            <th>Failure</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderTraceLane(items) {
  const normalized = items.map(parseRecord);
  if (!normalized.length) {
    return '<div class="empty-state">No trace found. Enter a transaction id to load the visual lane.</div>';
  }
  return normalized.map((item, index) => {
    const detection = isDetectionEvent(item);
    const status = String(item.status || '').toLowerCase();
    const anomaly = Boolean(item.metadata?.signals?.degradation?.anomaly || item.metadata?.signals?.failure || item.metadata?.root_cause);
    const tone = detection && (status === 'failure' || anomaly) ? 'failure' : detection ? 'detection' : 'ingestion';
    return `
      <article class="trace-card ${tone}">
        <div class="trace-top">
          <span class="pill">${escapeHtml(tone)}</span>
          <span class="trace-step">Step ${index + 1}</span>
        </div>
        <div class="trace-title">${escapeHtml(item.event_type || item.stage || 'event')}</div>
        <div class="trace-meta">${escapeHtml(item.timestamp || item._time || '')}</div>
        <div class="trace-sub">${escapeHtml(item.service || item.sourcetype || '')}</div>
        <div class="trace-sub">${escapeHtml(item.transaction_id || '')}</div>
      </article>
    `;
  }).join('');
}

async function fetchJSON(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
  return res.json();
}

async function loadStats() {
  const data = await fetchJSON('/api/stats');
  $('stats').innerHTML = '';
  data.rows.forEach((row) => {
    const el = document.createElement('div');
    el.className = 'metric';
    el.textContent = `${row.sourcetype || row.stage || row.event_type || 'metric'} | ${row.status || ''} | ${row.count}`;
    $('stats').appendChild(el);
  });
}

async function loadTables() {
  const [events, detections] = await Promise.all([
    fetchJSON('/api/events'),
    fetchJSON('/api/detections'),
  ]);

  const eventOut = $('event-table');
  const detectionOut = $('detection-table');
  eventOut.innerHTML = renderTable(events.items || [], 'ingestion');
  detectionOut.innerHTML = renderTable(detections.items || [], 'detection');

  document.querySelectorAll('[data-record]').forEach((row) => {
    row.addEventListener('click', () => {
      $('detail-view').textContent = JSON.stringify(parseRecord(JSON.parse(row.dataset.record)), null, 2);
    });
  });
}

async function runSearch() {
  const params = new URLSearchParams({
    transaction_id: $('transaction_id').value,
    customer_id: $('customer_id').value,
    stage: $('stage').value,
    status: $('status').value,
    event_type: $('event_type').value,
    failure_code: $('failure_code').value,
  });
  const data = await fetchJSON(`/api/search?${params.toString()}`);
  const out = $('search-results');
  out.innerHTML = '';
  out.innerHTML = renderTraceLane(data.items || []);
}

async function loadTrace() {
  const txn = $('trace-txn').value.trim();
  if (!txn) return;
  const data = await fetchJSON(`/api/trace?transaction_id=${encodeURIComponent(txn)}`);
  const out = $('trace-results');
  out.innerHTML = renderTraceLane(data.items || []);
  if (data.items?.[0]) $('detail-view').textContent = JSON.stringify(parseRecord(data.items[0]), null, 2);
}

async function refresh() {
  try {
    await Promise.all([loadStats(), loadTables()]);
    $('last-refresh').textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (err) {
    $('last-refresh').textContent = `Waiting for data: ${err.message}`;
  }
}

$('search-btn').addEventListener('click', runSearch);
$('trace-btn').addEventListener('click', loadTrace);
refresh();
setInterval(refresh, 5000);
