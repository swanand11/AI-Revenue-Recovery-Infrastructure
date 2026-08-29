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
    { label: 'Ingestion event', match: (item) => item.sourcetype === 'revtrace:mock-flow' || item.sourcetype === 'revtrace:seed' },
    { label: 'Detection event', match: (item) => item.sourcetype === 'revtrace:detection' },
    { label: 'Dashboard visible', match: (item) => item.sourcetype === 'revtrace:detection' || item.sourcetype === 'revtrace:mock-flow' },
  ];
  return phases.map((phase) => {
    const hit = items.find(phase.match);
    return {
      label: phase.label,
      state: hit ? 'seen' : 'missing',
      detail: hit ? `${hit.event_type || ''} from ${hit.service || hit.sourcetype}` : 'No matching event found in Splunk',
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
  $('lifecycle-state').textContent = ok ? 'Working as intended' : 'Broken or partial';
}

$('load-lifecycle').addEventListener('click', loadLifecycle);
