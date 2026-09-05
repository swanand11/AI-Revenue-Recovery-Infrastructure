const html = (value) => String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#39;');
const money = (value) => `INR ${Number(value || 0).toLocaleString()}`;

async function loadCustomerView() {
  const response = await fetch('/api/customer-view');
  const payload = await response.json();
  const rows = payload.items || [];
  const host = document.getElementById('customer-content');
  if (!rows.length) {
    host.innerHTML = '<div class="empty-state">No customer payment-link notifications have been sent yet.</div>';
    return;
  }
  host.innerHTML = rows.map((row) => `
    <article class="card portal-card">
      <div class="pill">${html(row.status)}</div>
      <h2>${html(row.notification)}</h2>
      <p class="subtle">Customer ${html(row.customer_id)} was notified for transaction ${html(row.transaction_id)}.</p>
      <div class="portal-steps">
        <span>Notification sent</span>
        <span>${html(row.payment_link)}</span>
        <span>${html(row.customer_action)}</span>
      </div>
      <strong>${html(money(row.amount_recovered))} recovered</strong>
    </article>
  `).join('');
}

loadCustomerView().catch((error) => {
  document.getElementById('customer-content').innerHTML = `<div class="empty-state">${html(error.message)}</div>`;
});

setInterval(loadCustomerView, 3000);
