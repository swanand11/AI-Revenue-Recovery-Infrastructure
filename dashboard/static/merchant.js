const html = (value) => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;');
const money = (value) => `INR ${Number(value || 0).toLocaleString()}`;

function statusClass(status) {
  const value = String(status || '').toUpperCase();
  if (value === 'FAILED') return 'bad';
  if (value === 'SUCCEEDED') return 'good';
  return 'wait';
}

async function loadMerchantView() {
  const response = await fetch('/api/merchant-view');
  const payload = await response.json();
  const rows = payload.batches || [];
  const host = document.getElementById('merchant-content');
  if (!rows.length) {
    host.innerHTML = '<div class="empty-state">No merchant settlement batches are available yet.</div>';
    return;
  }
  host.innerHTML = rows.map((row) => `
    <article class="card portal-card ${statusClass(row.status)}">
      <div class="status-badge ${statusClass(row.status)}">${html(row.status)}</div>
      <h2>Batch ${html(row.batch_id)}</h2>
      <p class="subtle">${html(row.merchant_message)}</p>
      <div class="portal-steps">
        <span>${html(row.transactions)} captured payments</span>
        <span>${html(money(row.settled_amount))} settled</span>
        <span>${html(money(row.revenue_at_risk))} at risk</span>
      </div>
      ${String(row.status).toUpperCase() === 'FAILED' ? `<a class="trail-link" href="/admin/money-trail?batch_id=${encodeURIComponent(row.batch_id)}">View customer to merchant trail</a>` : ''}
    </article>
  `).join('');
}

loadMerchantView().catch((error) => {
  document.getElementById('merchant-content').innerHTML = `<div class="empty-state">${html(error.message)}</div>`;
});

setInterval(loadMerchantView, 3000);
