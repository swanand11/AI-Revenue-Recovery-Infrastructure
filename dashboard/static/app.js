const $ = (id) => document.getElementById(id);

function renderEvent(item) {
  const el = document.createElement('div');
  el.className = 'event';
  el.innerHTML = `
    <div><span class="pill">${item.stage || item.sourcetype || 'event'}</span>${item.event_type || 'unknown'}</div>
    <div class="subtle">${item.transaction_id || ''}</div>
    <div class="subtle">${item.timestamp || item._time || ''}</div>
  `;
  el.addEventListener('click', () => {
    $('detail-view').textContent = JSON.stringify(item, null, 2);
  });
  return el;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}

function renderTable(items) {
  if (!items.length) {
    return '<div class="empty-state">No historical records found yet.</div>';
  }
  const rows = items.map((item) => `
    <tr data-record='${escapeHtml(JSON.stringify(item))}'>
      <td>${escapeHtml(item._time || item.timestamp || '')}</td>
      <td>${escapeHtml(item.stage || item.sourcetype || '')}</td>
      <td>${escapeHtml(item.event_type || '')}</td>
      <td>${escapeHtml(item.status || '')}</td>
      <td>${escapeHtml(item.transaction_id || '')}</td>
      <td>${escapeHtml(item.customer_id || '')}</td>
      <td>${escapeHtml(item.failure_code || '')}</td>
    </tr>
  `).join('');
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

async function loadLocalRecords() {
  const data = await fetchJSON('/api/recent');
  const out = $('local-records');
  if (!out) return;
  out.innerHTML = renderTable(data.items || []);
  out.querySelectorAll('tr[data-record]').forEach((row) => {
    row.addEventListener('click', () => {
      $('detail-view').textContent = JSON.stringify(JSON.parse(row.dataset.record), null, 2);
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
  data.items.forEach((item) => out.appendChild(renderEvent(item)));
}

async function loadTrace() {
  const txn = $('trace-txn').value.trim();
  if (!txn) return;
  const data = await fetchJSON(`/api/trace?transaction_id=${encodeURIComponent(txn)}`);
  const out = $('trace-results');
  out.innerHTML = '';
  data.items.forEach((item) => out.appendChild(renderEvent(item)));
  if (data.items[0]) $('detail-view').textContent = JSON.stringify(data.items[0], null, 2);
}

async function refresh() {
  try {
    await loadStats();
    await loadLocalRecords();
    $('last-refresh').textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (err) {
    $('last-refresh').textContent = `Waiting for data: ${err.message}`;
  }
}

$('search-btn').addEventListener('click', runSearch);
$('trace-btn').addEventListener('click', loadTrace);
refresh();
setInterval(refresh, 5000);
