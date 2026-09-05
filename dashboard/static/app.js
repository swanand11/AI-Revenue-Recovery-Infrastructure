const money = (value) => `INR ${Number(value || 0).toLocaleString()}`;
const escapeHtml = (value) => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;');

function renderImpact(impact) {
  const counts = [['Transactions processed', impact.transactions_processed], ['Failures', impact.failures], ['Recoverable', impact.recoverable], ['Recovered', impact.recovered], ['Escalated', impact.escalated]];
  document.getElementById('impact-cards').innerHTML = counts.map(([label, value]) => `<div class="impact"><strong>${escapeHtml(value || 0)}</strong><span>${escapeHtml(label)}</span></div>`).join('');
  const amounts = [['Amount at risk', impact.amount_at_risk], ['Amount recovered', impact.amount_recovered], ['Amount captured', impact.amount_captured], ['Amount settled', impact.amount_settled], ['Settlement at risk', impact.settlement_at_risk]];
  document.getElementById('money-cards').innerHTML = amounts.map(([label, value]) => `<div class="money"><span>${escapeHtml(label)}</span><strong>${money(value)}</strong></div>`).join('');
}

function renderRecovery(item) {
  return `<article class="card"><div class="flow-header"><div><div class="eyebrow">${escapeHtml(item.action || item.decision || 'RECOVERY')}</div><h2 style="margin-top:6px">${escapeHtml(item.transaction_id || item.transaction)}</h2></div><span class="badge ${item.status === 'RECOVERED' ? '' : 'failed'}">${escapeHtml(item.status || 'PENDING')}</span></div><div class="decision"><span>ACTION</span><strong>${escapeHtml(item.action || item.decision || 'PENDING')}</strong><span>${escapeHtml(item.guardrails || '')}</span></div><div class="flow-meta"><span>Provider: <strong>${escapeHtml(item.provider_before || 'unknown')} -> ${escapeHtml(item.provider_after || 'unknown')}</strong></span><span>Recovered: <strong>${money(item.amount_recovered)}</strong></span></div></article>`;
}

function renderSettlement(item) {
  if (!item) return '<div class="card empty">Settlement batches appear after captured payments reach simulated T+2.</div>';
  const complaint = item.complaint || {};
  return `<article class="card settlement-card"><div class="flow-header"><div><div class="eyebrow">SETTLEMENT</div><h2 style="margin-top:6px">${escapeHtml(item.batch_id)}</h2></div><span class="badge ${item.status === 'FAILED' ? 'failed' : ''}">${escapeHtml(item.status)}</span></div><div class="settlement-facts"><div class="fact"><span>Transactions</span><strong>${Number(item.transaction_count || 0).toLocaleString()}</strong></div><div class="fact"><span>Captured</span><strong>${money(item.gross_captured_amount)}</strong></div><div class="fact"><span>Settled</span><strong>${money(item.settled_amount)}</strong></div><div class="fact"><span>At risk</span><strong>${money(item.amount_at_stake)}</strong></div><div class="fact"><span>RCA</span><strong>${escapeHtml(item.rca?.candidate_root_cause || 'pending')}</strong></div></div>${complaint.messages ? `<details class="audit"><summary>VIEW ESCALATION</summary><div class="message" style="margin-top:10px">${escapeHtml(complaint.messages.bank || '')}</div></details>` : ''}</article>`;
}

async function refresh() {
  try {
    const response = await fetch('/api/admin');
    const data = await response.json();
    renderImpact(data.overview || {});
    document.getElementById('flows').innerHTML = (data.customer_recovery || []).map(renderRecovery).join('') || '<div class="card empty">Recovery cases appear after failure detections are processed.</div>';
    document.getElementById('settlement').innerHTML = (data.settlement || []).slice(-2).reverse().map(renderSettlement).join('') || renderSettlement(null);
    document.getElementById('demo-status').textContent = 'Observing live backend state';
    document.getElementById('demo-detail').textContent = `${data.ingestion?.length || 0} recent events materialized for the dashboard.`;
  } catch (error) {
    document.getElementById('demo-status').textContent = 'Waiting for backend state';
    document.getElementById('demo-detail').textContent = error.message;
  }
}

refresh();
setInterval(refresh, 3000);
