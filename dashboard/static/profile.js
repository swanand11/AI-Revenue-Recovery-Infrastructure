document.getElementById('profile-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const customerId = document.getElementById('customer-id').value.trim();
    if (!customerId) return;
    
    setProfileState('loading');
    
    try {
        const res = await fetch(`/api/profile?customer_id=${encodeURIComponent(customerId)}`);
        const data = await res.json();
        
        if (!res.ok) throw new Error(data.error || 'Failed to fetch profile');
        
        renderProfile(data);
        setProfileState('success');
    } catch (err) {
        setProfileState('error', err.message);
    }
});

if (document.getElementById('recovery-form')) document.getElementById('recovery-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const txnId = document.getElementById('transaction-id').value.trim();
    if (!txnId) return;
    
    setRecoveryState('loading');
    
    try {
        const res = await fetch(`/api/recovery?transaction_id=${encodeURIComponent(txnId)}`);
        const data = await res.json();
        
        if (!res.ok) throw new Error(data.error || 'Failed to fetch recovery state');
        
        renderRecovery(data);
        setRecoveryState('success');
    } catch (err) {
        setRecoveryState('error', err.message);
    } finally {
        if (document.getElementById('loading').style.display === 'block') document.getElementById('loading').style.display = 'none';
    }
});

function setProfileState(state, message = '') {
    document.getElementById('profile-idle').classList.toggle('hidden', state !== 'idle');
    document.getElementById('profile-loading').classList.toggle('hidden', state !== 'loading');
    document.getElementById('profile-error').classList.toggle('hidden', state !== 'error');
    document.getElementById('profile-results').classList.toggle('hidden', state !== 'success');
    document.getElementById('history-panel').classList.toggle('hidden', state !== 'success');
    document.getElementById('profile-error').textContent = message;
}

function setRecoveryState(state, message = '') {
    document.getElementById('recovery-idle').classList.toggle('hidden', state !== 'idle');
    document.getElementById('recovery-assessment-container').style.display = state === 'success' ? 'block' : 'none';
    document.getElementById('error').style.display = state === 'error' ? 'block' : 'none';
    if (state === 'error') document.getElementById('error').textContent = message;
}

function renderRecovery(state) {
    document.getElementById('results-container').style.display = 'block';
    const container = document.getElementById('recovery-assessment-container');
    container.style.display = 'block';
    renderConsensus(state.consensus || {});
    renderPolicy(state.policy || {});
    
    const grid = document.getElementById('agent-beliefs-grid');
    grid.innerHTML = '';
    document.getElementById('recovery-empty').style.display = 'none';
    
    if (!state.agent_beliefs || state.agent_beliefs.length === 0) {
        document.getElementById('recovery-empty').style.display = 'block';
        document.getElementById('consensus-panel').style.display = 'none';
        document.getElementById('policy-panel').style.display = 'none';
        return;
    }
    
    state.agent_beliefs.forEach((belief, idx) => {
        const el = document.createElement('div');
        el.className = 'glass-card animate-in';
        el.style.animationDelay = `${0.1 + (idx * 0.1)}s`;
        
        let confidenceColor = '#94a3b8';
        if (belief.confidence >= 0.8) confidenceColor = '#10b981';
        else if (belief.confidence >= 0.5) confidenceColor = '#f59e0b';
        else confidenceColor = '#ef4444';
        
        el.innerHTML = `
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 10px;">
                <h3 style="margin: 0; color: #38bdf8;">${belief.agent_id} <span style="font-size: 0.8em; color: #64748b; font-weight: normal;">(${belief.agent_version})</span></h3>
                <span class="pill" style="background: ${confidenceColor}; color: white; border: none;">${Math.round(belief.confidence * 100)}% Conf</span>
            </div>
            <div style="margin-bottom: 15px;">
                <div style="font-size: 0.85em; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px;">Recommendation</div>
                <div style="font-size: 1.2em; font-weight: 600; color: white;">${belief.recommendation}</div>
            </div>
            <div style="margin-bottom: 15px;">
                <div style="font-size: 0.85em; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 4px;">Reason</div>
                <div style="color: #cbd5e1;">${belief.reason_code}</div>
            </div>
            <div style="background: rgba(0,0,0,0.2); padding: 12px; border-radius: 8px; font-family: monospace; font-size: 0.85em; color: #94a3b8; overflow-x: auto;">
                ${JSON.stringify(belief.evidence || {}, null, 2).replace(/\\n/g, '<br>').replace(/ /g, '&nbsp;')}
            </div>
        `;
        grid.appendChild(el);
    });
}

function renderPolicy(policy) {
    const panel = document.getElementById('policy-panel');
    if (!policy || Object.keys(policy).length === 0) { panel.style.display = 'none'; return; }
    panel.style.display = 'block';
    const list = document.getElementById('guardrail-list');
    list.innerHTML = (policy.checks || []).map((check) => `
        <div style="display:flex; justify-content:space-between; gap:12px; color:#cbd5e1;">
            <span>${check.name || check.check || 'Guardrail'}</span><strong style="color:${check.passed ? '#6ee7b7' : '#fca5a5'}">${check.passed ? 'PASS' : 'BLOCK'}</strong>
        </div>`).join('');
    const status = document.getElementById('policy-status');
    status.textContent = policy.allowed ? 'ASSESSMENT PASS' : 'ASSESSMENT BLOCKED';
    status.style.background = policy.allowed ? 'rgba(16,185,129,.2)' : 'rgba(239,68,68,.2)';
    status.style.color = policy.allowed ? '#6ee7b7' : '#fca5a5';
}

function renderConsensus(consensus) {
    const panel = document.getElementById('consensus-panel');
    if (!consensus || Object.keys(consensus).length === 0) {
        panel.style.display = 'none';
        return;
    }
    panel.style.display = 'block';

    const support = Math.round((consensus.support_ratio || 0) * 100);
    const threshold = Math.round((consensus.threshold || 0) * 100);
    const status = consensus.decision_status || 'UNKNOWN';
    const statusEl = document.getElementById('consensus-status');

    document.getElementById('consensus-decision').textContent = consensus.decision || 'DO_NOTHING';
    document.getElementById('consensus-support').textContent = `${support}%`;
    document.getElementById('consensus-threshold').textContent = `${threshold}%`;
    document.getElementById('consensus-valid-agents').textContent = `${consensus.valid_agent_count || 0}/${consensus.minimum_agent_count || 0}`;
    document.getElementById('consensus-conflicts').textContent = consensus.conflicts_detected ? 'YES' : 'NO';

    let statusColor = '#94a3b8';
    if (status === 'QUORUM_REACHED') statusColor = '#10b981';
    else if (status === 'NO_QUORUM' || status === 'INSUFFICIENT_EVIDENCE') statusColor = '#f59e0b';
    else if (status.includes('BLOCK') || status.includes('REJECT')) statusColor = '#ef4444';
    statusEl.textContent = status;
    statusEl.style.background = statusColor;
    statusEl.style.color = 'white';
    statusEl.style.border = 'none';

    const bars = document.getElementById('support-bars');
    bars.innerHTML = '';
    Object.entries(consensus.support_by_action || {}).forEach(([action, ratio]) => {
        const pct = Math.round((ratio || 0) * 100);
        const row = document.createElement('div');
        row.innerHTML = `
            <div style="display: flex; justify-content: space-between; color: #cbd5e1; font-size: 0.9em; margin-bottom: 5px;">
                <span>${action}</span>
                <span>${pct}%</span>
            </div>
            <div style="height: 10px; border-radius: 999px; background: rgba(255,255,255,0.08); overflow: hidden;">
                <div style="height: 100%; width: ${Math.min(100, pct)}%; background: ${pct >= threshold ? '#10b981' : '#38bdf8'};"></div>
            </div>
        `;
        bars.appendChild(row);
    });

    const table = document.getElementById('agent-vote-table');
    const rows = (consensus.agent_results || []).map((agent) => `
        <tr>
            <td>${agent.agent_id || '-'}</td>
            <td>${agent.recommendation || '-'}</td>
            <td>${Math.round((agent.confidence || 0) * 100)}%</td>
            <td>${agent.status || '-'}</td>
            <td>${agent.reason || ''}</td>
        </tr>
    `).join('');
    table.innerHTML = `
        <table class="records-table" style="min-width: 720px;">
            <thead>
                <tr>
                    <th>Agent</th>
                    <th>Vote</th>
                    <th>Confidence</th>
                    <th>BFT Status</th>
                    <th>Reason</th>
                </tr>
            </thead>
            <tbody>${rows}</tbody>
        </table>
    `;
}

function renderProfile(data) {
    document.getElementById('results-container').style.display = 'block';
    
    const intent = data.intent;
    
    const hs = intent.history_status || 'UNKNOWN';
    const qualityEl = document.getElementById('data-quality');
    const qualityColors = {
        HISTORY_FOUND:       { bg: 'rgba(16,185,129,0.15)', fg: '#10b981' },
        NO_HISTORY:          { bg: 'rgba(148,163,184,0.15)', fg: '#94a3b8' },
        PARTIAL_HISTORY:     { bg: 'rgba(245,158,11,0.15)',  fg: '#f59e0b' },
        HISTORY_PARSE_ERROR: { bg: 'rgba(239,68,68,0.15)',   fg: '#ef4444' },
        HISTORY_QUERY_ERROR: { bg: 'rgba(239,68,68,0.15)',   fg: '#ef4444' },
    };
    const qc = qualityColors[hs] || qualityColors.NO_HISTORY;
    qualityEl.textContent = hs.replace(/_/g, ' ');
    qualityEl.style.background = qc.bg;
    qualityEl.style.color = qc.fg;

    // ── Score circle ──────────────────────────────────────────────────
    const scoreCircle = document.getElementById('intent-score');
    let scoreStr = intent.intent_score.toFixed(1);
    if (intent.intent_score > 0) scoreStr = `+${scoreStr}`;
    scoreCircle.textContent = scoreStr;
    
    if (intent.intent_score > 0) {
        scoreCircle.style.borderColor = '#10b981';
        scoreCircle.style.boxShadow = '0 0 30px rgba(16, 185, 129, 0.2), inset 0 0 20px rgba(16, 185, 129, 0.1)';
    } else if (intent.intent_score < 0) {
        scoreCircle.style.borderColor = '#ef4444';
        scoreCircle.style.boxShadow = '0 0 30px rgba(239, 68, 68, 0.2), inset 0 0 20px rgba(239, 68, 68, 0.1)';
    } else {
        scoreCircle.style.borderColor = '#94a3b8';
        scoreCircle.style.boxShadow = '0 0 30px rgba(148, 163, 184, 0.2), inset 0 0 20px rgba(148, 163, 184, 0.1)';
    }
    
    document.getElementById('intent-level').textContent = (intent.intent_level || 'NEUTRAL').replace(/_/g, ' ');
    document.getElementById('intent-confidence').textContent = Math.round((intent.confidence || 0) * 100);
    
    // ── Stats ─────────────────────────────────────────────────────────
    document.getElementById('stat-total').textContent = intent.historical_transactions || 0;
    document.getElementById('stat-success').textContent = intent.successful_transactions || 0;
    document.getElementById('stat-failures').textContent = intent.failed_transactions || 0;
    document.getElementById('stat-retries').textContent = intent.retries || 0;
    
    document.getElementById('breakdown-checkout').textContent = intent.checkout_failures || 0;
    document.getElementById('breakdown-payment').textContent = intent.payment_failures || 0;
    document.getElementById('breakdown-auth').textContent = intent.authorization_failures || 0;
    document.getElementById('breakdown-capture').textContent = intent.capture_failures || 0;
    document.getElementById('breakdown-settlement').textContent = intent.settlement_failures || 0;

    const timeline = document.getElementById('timeline');
    timeline.innerHTML = '';
    
    if (!data.history || data.history.length === 0) {
        timeline.innerHTML = '<div style="color: #94a3b8; padding: 20px 0;">No historical events found for this customer.</div>';
        return;
    }
    
    data.history.forEach((event, idx) => {
        const el = document.createElement('div');
        el.className = 'timeline-event animate-in';
        el.style.animationDelay = `${0.3 + (idx * 0.05)}s`;
        
        const eventType = event.event_type || '';
        const stage = event.stage || 'Unknown';
        
        const isSuccess = eventType.endsWith('_succeeded') || eventType === 'checkout_completed';
        const isFailure = eventType.endsWith('_failed');
        
        if (isSuccess) el.classList.add('success');
        if (isFailure) el.classList.add('failure');
        
        let pillClass = 'pill';
        if (isSuccess) pillClass += ' success';
        if (isFailure) pillClass += ' failure';
        
        const statusText = isSuccess ? 'Success' : isFailure ? 'Failed' : event.status || 'Event';
        const timeStr = event.timestamp || 'N/A';
        
        el.innerHTML = `
            <div class="timeline-content">
                <div class="event-header">
                    <span class="event-title">${stage} <span style="font-weight: 400; color: #94a3b8; font-size: 0.9em; margin-left: 5px;">(${eventType})</span></span>
                    <span class="${pillClass}">${statusText}</span>
                </div>
                <div class="event-meta">
                    <div style="margin-bottom: 4px;"><strong style="color: white;">Transaction ID:</strong> ${event.transaction_id || 'N/A'}</div>
                    <div style="display: flex; justify-content: space-between;">
                        <span><strong style="color: white;">Time:</strong> ${timeStr}</span>
                        ${event.failure_code ? `<span style="color: #ef4444;">[ ${event.failure_code} ]</span>` : ''}
                    </div>
                </div>
            </div>
        `;
        timeline.appendChild(el);
    });
}
