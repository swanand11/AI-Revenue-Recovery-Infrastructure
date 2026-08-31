const $ = (id) => document.getElementById(id);

function statusClass(state) {
  if (state === 'seen') return 'ok';
  if (state === 'missing') return 'bad';
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
  const phases = [
    { label: 'checkout', match: (item) => item.stage === 'checkout' || item.event_type === 'checkout_started' },
    { label: 'payment', match: (item) => item.stage === 'payment' || item.event_type === 'payment_created' },
    { label: 'auth', match: (item) => item.stage === 'authorization' || item.event_type === 'authorization_requested' },
    { label: 'capture', match: (item) => item.stage === 'capture' || item.event_type === 'capture_requested' },
    { label: 'settlement', match: (item) => item.stage === 'settlement' || item.event_type === 'settlement_initiated' },
  ];
  return phases.map((phase) => {
    const hit = items.find(phase.match);
    return {
      label: phase.label,
      state: hit ? 'seen' : 'missing',
      detail: hit ? `${hit.event_type || ''} from ${hit.service || hit.sourcetype}` : 'No matching event found in the transaction timeline',
      timestamp: hit ? hit.timestamp : '',
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
  const ok = rows.every((row) => row.state === 'seen');
  $('lifecycle-state').textContent = ok ? 'Lifecycle aligned' : 'Lifecycle partial';
}

$('load-lifecycle').addEventListener('click', loadLifecycle);
