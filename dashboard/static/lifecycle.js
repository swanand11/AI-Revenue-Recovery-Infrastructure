const $ = (id) => document.getElementById(id);

function statusClass(state) {
  if (state === 'seen') return 'ok';
  if (state === 'failed' || state === 'missing') return 'bad';
  return 'warn';
}

function renderLifecycleRow(row) {
  const el = document.createElement('div');
  el.className = `flow-item ${statusClass(row.state)}`;
  el.innerHTML = `
    <div class="flow-head">
      <span class="pill">${row.state}</span>
      <strong>${row.label}</strong>
    </div>
    <div class="subtle">${row.detail}</div>
    <div class="flow-meta">${row.timestamp || ''}</div>
  `;
  return el;
}

async function fetchJSON(path) {
  const res = await fetch(path);
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
  return res.json();
}

function buildLifecycle(items) {
  return items.map((item, index) => {
    const failed = item.status === 'failure' || item.status === 'unknown';
    const detection = item.detection;
    return {
      label: `Step ${index + 1}: ${item.event_type || item.stage || 'source event'}`,
      state: failed ? 'failed' : 'seen',
      detail: `${item.status || ''}${item.failure_code ? ` · ${item.failure_code}` : ''}${detection ? ` · Detection: ${detection.metadata?.root_cause?.component || 'analyzed'}` : ''}`,
      timestamp: item.timestamp || item._time || '',
    };
  });
}

async function loadLifecycle() {
  const txn = $('txn').value.trim();
  if (!txn) return;
  const data = await fetchJSON(`/api/lifecycle?transaction_id=${encodeURIComponent(txn)}`);
  const rows = buildLifecycle(data.items || []);
  const out = $('lifecycle-items');
  out.innerHTML = '';
  rows.forEach((row) => out.appendChild(renderLifecycleRow(row)));
  const ok = rows.length > 0 && rows.every((row) => row.state === 'seen');
  $('lifecycle-state').textContent = ok ? 'Lifecycle complete' : rows.length ? 'Failure point shown' : 'No source events';
}

$('load-lifecycle').addEventListener('click', loadLifecycle);
