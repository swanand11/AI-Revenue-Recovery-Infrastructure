document.getElementById('profile-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const customerId = document.getElementById('customer-id').value.trim();
    if (!customerId) return;
    
    document.getElementById('loading').style.display = 'block';
    document.getElementById('results-container').style.display = 'none';
    document.getElementById('error').style.display = 'none';
    
    try {
        const res = await fetch(`/api/profile?customer_id=${encodeURIComponent(customerId)}`);
        const data = await res.json();
        
        if (!res.ok) throw new Error(data.error || 'Failed to fetch profile');
        
        renderProfile(data);
    } catch (err) {
        document.getElementById('error').textContent = err.message;
        document.getElementById('error').style.display = 'block';
    } finally {
        document.getElementById('loading').style.display = 'none';
    }
});

document.getElementById('recovery-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const txnId = document.getElementById('transaction-id').value.trim();
    if (!txnId) return;
    
    document.getElementById('loading').style.display = 'block';
    document.getElementById('results-container').style.display = 'none';
    document.getElementById('error').style.display = 'none';
    
    try {
        const res = await fetch(`/api/recovery?transaction_id=${encodeURIComponent(txnId)}`);
        const data = await res.json();
        
        if (!res.ok) throw new Error(data.error || 'Failed to fetch recovery state');
        
        renderRecovery(data);
    } catch (err) {
        document.getElementById('error').textContent = err.message;
        document.getElementById('error').style.display = 'block';
    } finally {
        document.getElementById('loading').style.display = 'none';
    }
});

function renderRecovery(state) {
    document.getElementById('results-container').style.display = 'block';
    const container = document.getElementById('recovery-assessment-container');
    container.style.display = 'block';
    
    const grid = document.getElementById('agent-beliefs-grid');
    grid.innerHTML = '';
    
    if (!state.agent_beliefs || state.agent_beliefs.length === 0) {
        grid.innerHTML = '<div style="color: #94a3b8; padding: 20px 0;">No agent beliefs found for this transaction.</div>';
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

function renderProfile(data) {
    document.getElementById('results-container').style.display = 'block';
    
    const intent = data.intent;
    
    // ── History-status banner ──────────────────────────────────────────
    const statusBanner = document.getElementById('history-status-banner');
    const hs = intent.history_status || 'UNKNOWN';
    if (['HISTORY_QUERY_ERROR', 'HISTORY_PARSE_ERROR', 'PARTIAL_HISTORY'].includes(hs)) {
        const labels = {
            HISTORY_QUERY_ERROR: '⚠️ Splunk query failed — scores may be unreliable.',
            HISTORY_PARSE_ERROR: '⚠️ Events retrieved but could not be parsed — scores may be incorrect.',
            PARTIAL_HISTORY: '⚠️ Some events could not be parsed — scores are based on partial data.',
        };
        statusBanner.textContent = labels[hs] || hs;
        statusBanner.style.display = 'block';
    } else {
        statusBanner.style.display = 'none';
    }

    // ── Data quality badge ────────────────────────────────────────────
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

    // ── Data quality detail ───────────────────────────────────────────
    document.getElementById('raw-event-count').textContent = intent.raw_event_count || 0;
    document.getElementById('parsed-event-count').textContent = intent.parsed_event_count || 0;
    document.getElementById('parse-error-count').textContent = intent.parse_error_count || 0;

    // ── Timeline ──────────────────────────────────────────────────────
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
