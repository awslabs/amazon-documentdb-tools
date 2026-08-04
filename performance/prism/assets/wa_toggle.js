// Expand/Collapse all — WA and Code Review panels
(function() {
    document.addEventListener('click', function(e) {
        // WA buttons
        var waExpand = e.target.closest('[id="wa2-expand-all"]');
        var waCollapse = e.target.closest('[id="wa2-collapse-all"]');
        if (waExpand || waCollapse) {
            var container = document.getElementById('wa2-results');
            if (container) {
                var details = container.getElementsByTagName('details');
                for (var i = 0; i < details.length; i++) {
                    details[i].open = !!waExpand;
                }
            }
            return;
        }

        // Code Review buttons
        var crExpand = e.target.closest('[id="cr-expand-all"]');
        var crCollapse = e.target.closest('[id="cr-collapse-all"]');
        if (crExpand || crCollapse) {
            var container = document.getElementById('cr-results-panels');
            if (container) {
                var details = container.getElementsByTagName('details');
                for (var i = 0; i < details.length; i++) {
                    details[i].open = !!crExpand;
                }
            }
            return;
        }
    });

    // Code Review: toggle button label on input focus/blur
    document.addEventListener('focusin', function(e) {
        if (e.target && e.target.id === 'code-review-target-dir') {
            var btn = document.getElementById('code-review-run-btn');
            if (btn) btn.textContent = '🔍 Scan';
        }
    });
})();

// Auth connect — client-side validation only (button state handled by Dash clientside callback)
document.addEventListener('click', function(e) {
    var btn = e.target.closest('[id="auth-connect-btn"]');
    if (!btn) return;

    // Quick client-side validation
    var username = document.getElementById('auth-username');
    var password = document.getElementById('auth-password');
    var manualConn = document.getElementById('auth-manual-conn');
    var hasManual = manualConn && manualConn.value && manualConn.value.trim();

    if (!hasManual) {
        var missing = [];
        if (!username || !username.value.trim()) missing.push('username');
        if (!password || !password.value.trim()) missing.push('password');

        // Check tunnel fields
        var modeInputs = document.querySelectorAll('[name="auth-conn-mode"]');
        var mode = 'direct';
        modeInputs.forEach(function(input) { if (input.checked) mode = input.value; });
        if (mode === 'tunnel') {
            var bastion = document.getElementById('auth-bastion');
            var sshKey = document.getElementById('auth-ssh-key');
            if (!bastion || !bastion.value.trim()) missing.push('bastion host');
            if (!sshKey || !sshKey.value.trim()) missing.push('SSH key path');
        }

        if (missing.length > 0) {
            var existing = document.getElementById('auth-js-validation');
            if (!existing) {
                existing = document.createElement('div');
                existing.id = 'auth-js-validation';
                existing.style.cssText = 'font-size:.85rem;color:#D13212;padding:.3rem 0';
                btn.parentNode.insertBefore(existing, btn.nextSibling);
            }
            existing.textContent = 'Missing: ' + missing.join(', ');
            return;
        }
    }

    // Clear any previous validation message
    var valMsg = document.getElementById('auth-js-validation');
    if (valMsg) valMsg.textContent = '';
    // NOTE: button disable/enable is handled by the Dash clientside callback,
    // which is synchronized with the server response. Do NOT manipulate
    // btn.disabled here — it caused the button to stay stuck on failure.
});

// Index Health — toggle between Needs Attention and All Indexes views
document.addEventListener('click', function(e) {
    var attnBtn = e.target.closest('[id="idx-toggle-attention"]');
    var allBtn = e.target.closest('[id="idx-toggle-all"]');
    if (!attnBtn && !allBtn) return;

    var attnView = document.getElementById('idx-view-attention');
    var allView = document.getElementById('idx-view-all');
    var attnToggle = document.getElementById('idx-toggle-attention');
    var allToggle = document.getElementById('idx-toggle-all');
    if (!attnView || !allView || !attnToggle || !allToggle) return;

    if (attnBtn) {
        attnView.style.display = 'block';
        allView.style.display = 'none';
        attnToggle.style.background = 'var(--color-warning)';
        attnToggle.style.color = '#fff';
        attnToggle.style.border = 'none';
        allToggle.style.background = 'var(--bg-surface-alt)';
        allToggle.style.color = 'var(--text-muted)';
        allToggle.style.border = '1px solid var(--border-default)';
    } else {
        attnView.style.display = 'none';
        allView.style.display = 'block';
        allToggle.style.background = 'var(--text-heading)';
        allToggle.style.color = '#fff';
        allToggle.style.border = 'none';
        attnToggle.style.background = 'var(--bg-surface-alt)';
        attnToggle.style.color = 'var(--text-muted)';
        attnToggle.style.border = '1px solid var(--border-default)';
    }
});
