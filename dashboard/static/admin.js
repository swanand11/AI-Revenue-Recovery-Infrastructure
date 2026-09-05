const routes = [
  ['admin', 'Overview'],
  ['admin/ingestion', 'Ingestion'],
  ['admin/detection', 'Detection'],
  ['admin/providers', 'Providers'],
  ['admin/recovery/customer', 'Customer Recovery'],
  ['admin/recovery/provider', 'Provider Recovery'],
  ['admin/settlement', 'Settlement'],
  ['admin/settlement/rca', 'Settlement RCA'],
  ['admin/money-trail', 'Money Trail'],
  ['admin/escalations', 'Escalations'],
  ['admin/audit', 'Audit'],
];

const routeKey = () => location.pathname.replace(/^\/+/, '') || 'admin';
const html = (value) => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;');
const money = (value) => `INR ${Number(value || 0).toLocaleString()}`;

function nav() {
  const current = routeKey();
  document.getElementById('admin-nav').innerHTML = routes.map(([path, label]) => `<a class="${path === current ? 'active' : ''}" href="/${path}">${label}</a>`).join('');
}

async function load() {
  nav();
  const response = await fetch('/api/admin');
  const state = await response.json();
  renderOverview(state.overview || {});
  renderPage(routeKey(), state);
}

function renderOverview(overview) {
  const metrics = [
    ['Transactions', overview.transactions_processed],
    ['Failures', overview.failures],
    ['Recovered', overview.recovered],
    ['Guardrail Blocked', overview.guardrail_blocked],
    ['Amount At Risk', money(overview.amount_at_risk)],
    ['Amount Recovered', money(overview.amount_recovered)],
    ['Captured', money(overview.amount_captured)],
    ['Settled', money(overview.amount_settled)],
    ['Settlement At Risk', money(overview.settlement_at_risk)],
    ['Recovery Rate', `${overview.recovery_success_rate || 0}%`],
    ['Revenue Rate', `${overview.revenue_recovery_rate || 0}%`],
    ['Escalations', overview.active_escalations],
  ];
  document.getElementById('overview-grid').innerHTML = metrics.map(([label, value]) => `<article class="admin-metric"><strong>${html(value)}</strong><span>${html(label)}</span></article>`).join('');
}

function title(text) {
  document.getElementById('page-title').textContent = text;
}

function table(rows, columns) {
  if (!rows.length) return '<div class="empty-state">No records available yet.</div>';
  return `<div class="table-scroll"><table class="admin-table"><thead><tr>${columns.map((col) => `<th>${html(col[1])}</th>`).join('')}</tr></thead><tbody>${rows.map((row) => `<tr>${columns.map(([key]) => `<td>${formatCell(row[key], key)}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
}

function formatCell(value, key) {
  if (key.includes('amount') || key.includes('risk') || key === 'money_impact') return html(money(value));
  if (Array.isArray(value)) return html(value.join(' -> '));
  if (typeof value === 'object' && value) return `<pre>${html(JSON.stringify(value, null, 2))}</pre>`;
  return html(value);
}

function statusBadge(status) {
  const value = String(status || 'UNKNOWN').toUpperCase();
  const tone = value === 'FAILED' ? 'bad' : value === 'SUCCEEDED' || value === 'RECOVERED' ? 'good' : 'wait';
  return `<span class="status-badge ${tone}">${html(value)}</span>`;
}

function settlementTable(rows) {
  if (!rows.length) return '<div class="empty-state">No settlement batches available yet.</div>';
  const cols = ['Batch', 'Date', 'Transactions', 'Remaining To 100', 'Captured', 'Settled', 'Failed', 'Pending', 'Revenue At Risk', 'Root Cause', 'Status', 'Action'];
  const body = rows.map((row) => {
    const failed = String(row.status || '').toUpperCase() === 'FAILED';
    const action = failed
      ? `<div class="settlement-actions"><a class="trail-link" href="/admin/money-trail?batch_id=${encodeURIComponent(row.batch_id)}">Money trail</a><a class="trail-link" href="/admin/settlement/rca">RCA & escalation</a></div>`
      : '<span class="muted">Monitoring</span>';
    return `<tr class="${failed ? 'failed-row' : ''}">
      <td>${html(row.batch_id)}</td>
      <td>${html(row.batch_date)}</td>
      <td>${html(row.transactions)}</td>
      <td>${html(row.remaining_to_ready ?? 0)}</td>
      <td>${html(money(row.captured_amount))}</td>
      <td>${html(money(row.settled_amount))}</td>
      <td>${html(money(row.failed_amount))}</td>
      <td>${html(money(row.pending_amount))}</td>
      <td>${html(money(row.revenue_at_risk))}</td>
      <td>${html(failed ? (row.candidate_root_cause || row.rca?.failure_code || 'Investigation pending') : '-')}</td>
      <td>${statusBadge(row.status)}</td>
      <td>${action}</td>
    </tr>`;
  }).join('');
  return `<div class="table-scroll"><table class="admin-table"><thead><tr>${cols.map((col) => `<th>${html(col)}</th>`).join('')}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function renderMoneyTrail(state) {
  title('Money Trail');
  const content = document.getElementById('content');
  const batchId = new URLSearchParams(location.search).get('batch_id');
  const trails = state.money_trails || [];
  const trail = trails.find((item) => item.batch_id === batchId) || trails.find((item) => item.status === 'FAILED') || trails[0];
  if (!trail) {
    content.innerHTML = '<div class="empty-state">No batch trail is available yet. Failed batches will appear here automatically.</div>';
    return;
  }
  const flow = (trail.flow || []).map((step, index) => `<div class="trail-node ${index === 3 && trail.status === 'FAILED' ? 'blocked' : ''}"><span>${index + 1}</span><strong>${html(step)}</strong></div>`).join('');
  const rows = table(trail.transactions || [], [['customer_id','Customer'], ['transaction_id','Transaction'], ['merchant_id','Merchant'], ['order_id','Order'], ['payment_id','Payment'], ['provider','Provider'], ['captured_amount','Captured'], ['settlement_status','Settlement']]);
  content.innerHTML = `
    <article class="admin-panel highlight-panel">
      <div class="panel-head">
        <div>
          <h2>${html(trail.batch_id)}</h2>
          <p class="subtle">Customer to merchant mapping derived from captured payments assigned to this settlement batch.</p>
        </div>
        ${statusBadge(trail.status)}
      </div>
      <div class="trail-map">${flow}</div>
      <div class="admin-grid compact">
        <article class="admin-metric"><strong>${html(money(trail.captured_amount))}</strong><span>Captured</span></article>
        <article class="admin-metric"><strong>${html(money(trail.settled_amount))}</strong><span>Settled</span></article>
        <article class="admin-metric danger"><strong>${html(money(trail.revenue_at_risk))}</strong><span>At Risk</span></article>
        <article class="admin-metric"><strong>${html(trail.summary?.merchants || 0)}</strong><span>Merchants</span></article>
      </div>
    </article>
    <article class="admin-panel"><h2>Mapped Transactions</h2>${rows}</article>`;
}

function renderPage(path, state) {
  const content = document.getElementById('content');
  if (path === 'admin') {
    title('Overview');
    content.innerHTML = '';
    return;
  }
  if (path.endsWith('detection')) {
    title('Detection');
    content.innerHTML = `<article class="admin-panel">${table(state.detections || [], [['detection_id','Detection'], ['transaction_id','Transaction'], ['stage','Stage'], ['failure','Failure'], ['intent','Intent'], ['median','Median'], ['degradation','Degradation'], ['rca','RCA'], ['confidence','Confidence'], ['recoverability','Recoverability']])}</article>`;
    return;
  }
  if (path.endsWith('ingestion')) {
    title('Live Ingestion');
    content.innerHTML = `<article class="admin-panel">${table(state.ingestion || [], [['timestamp','Timestamp'], ['event_id','Event ID'], ['service','Service'], ['stage','Stage'], ['event_type','Event Type'], ['transaction_id','Transaction'], ['payment_id','Payment'], ['amount','Amount'], ['status','Status'], ['failure_code','Failure'], ['trace_id','Trace']])}</article>`;
    return;
  }
  if (path.endsWith('providers')) {
    title('Providers');
    content.innerHTML = `<article class="admin-panel">${table(state.providers || [], [['provider','Provider'], ['payment_method','Method'], ['health','Health'], ['success_rate','Success Rate'], ['failure_rate','Failure Rate'], ['timeout_rate','Timeout Rate'], ['latency_ms','Latency MS'], ['degradation_probability','Degradation Probability'], ['recommended_provider','Recommended Provider'], ['switch_attempts','Switch Attempts'], ['successful_switches','Successful Switches'], ['state','State']])}</article>`;
    return;
  }
  if (path.endsWith('recovery/customer')) {
    title('Customer Recovery');
    content.innerHTML = `<article class="admin-panel">${table(state.customer_recovery || [], [['transaction_id','Transaction'], ['customer_id','Customer'], ['intent_score','Intent'], ['current_median','Median'], ['intent_bucket','Bucket'], ['recommended_action','Recommended Action'], ['notification_status','Notification'], ['payment_link_status','Payment Link'], ['links_sent','Links Sent'], ['links_opened','Links Opened'], ['payment_attempts','Payment Attempts'], ['successful_captures','Successful Captures'], ['conversion_rate','Conversion %'], ['amount_recovered','Recovered Amount'], ['status','Status']])}</article>`;
    return;
  }
  if (path.endsWith('recovery/provider')) {
    title('Provider Recovery');
    content.innerHTML = `<article class="admin-panel">${table(state.provider_recovery || [], [['failure','Failure'], ['agent_beliefs','Agent Beliefs'], ['consensus','Consensus'], ['guardrails','Guardrails'], ['action','Action'], ['provider_before','Before'], ['provider_after','After'], ['retry_number','Attempt'], ['payment_result','Payment'], ['authorization_result','Authorization'], ['capture_result','Capture'], ['amount_recovered','Recovered']])}</article>`;
    return;
  }
  if (path.endsWith('settlement/rca')) {
    title('Settlement RCA');
    content.innerHTML = `<article class="admin-panel">${table(state.settlement_rca || [], [['batch_id','Batch'], ['candidate_root_cause','Candidate Cause'], ['confidence','Confidence'], ['failure_code','Failure Code'], ['transaction_count','Transactions'], ['failure_count','Failed'], ['revenue_at_risk','Revenue At Risk'], ['evidence','Evidence']])}</article>`;
    return;
  }
  if (path.endsWith('money-trail')) {
    renderMoneyTrail(state);
    return;
  }
  if (path.endsWith('settlement')) {
    title('Settlement');
    content.innerHTML = `<article class="admin-panel">${settlementTable(state.settlement || [])}</article>`;
    return;
  }
  if (path.endsWith('escalations')) {
    title('Escalations');
    const rows = state.escalations || [];
    content.innerHTML = `<article class="admin-panel">${table(rows, [['escalation_id','Escalation'], ['batch_id','Batch'], ['merchant_id','Merchant'], ['bank_name','Bank'], ['severity','Severity'], ['status','Status'], ['current_owner','Current Owner'], ['next_action','Next Action'], ['llm_summary','LLM Summary']])}</article>${rows.map((row) => `<article class="admin-panel escalation-card">
      <div class="panel-head"><div><h2>${html(row.escalation_id || row.complaint_id)}</h2><p class="danger-text">${html(row.highlight || '')}</p></div>${statusBadge(row.severity)}</div>
      <div class="chain">${(row.audit_chain || row.chain || []).map((step) => `<span>${html(step)}</span>`).join('')}</div>
      <div class="split" style="margin-top:14px">
        <section class="message-card"><h2>Bank Message</h2><pre>${html(row.messages?.bank || '')}</pre></section>
        <section class="message-card"><h2>Merchant Message</h2><pre>${html(row.messages?.merchant || '')}</pre></section>
      </div>
    </article>`).join('')}`;
    return;
  }
  if (path.endsWith('audit')) {
    title('Audit');
    content.innerHTML = `<article class="admin-panel">${table(state.audit || [], [['timestamp','Timestamp'], ['event_type','Event'], ['event','Decision Event'], ['transaction_id','Transaction'], ['batch_id','Batch'], ['complaint_id','Complaint'], ['action','Action'], ['result','Result'], ['money_impact','Money Impact']])}</article>`;
  }
}

load().catch((error) => {
  document.getElementById('content').innerHTML = `<div class="empty-state">${html(error.message)}</div>`;
});

setInterval(load, 3000);
