const state = {
    currentStorageId: document.getElementById('selected-storage-id')?.value || null,
    currentAuthMethod: document.getElementById('selected-auth-method')?.value || null,
    revealToken: null,
    lastMaskedResponse: null
};

let autoMaskTimer = null;

function renderDecryptResult(html, tone = 'default') {
    const resultBox = document.getElementById('decrypt-result');
    if (!resultBox) {
        return;
    }
    const toneClasses = {
        default: 'border-slate-100 bg-slate-50 text-slate-700',
        success: 'border-emerald-200 bg-emerald-50 text-emerald-900',
        warning: 'border-amber-200 bg-amber-50 text-amber-900',
        error: 'border-rose-200 bg-rose-50 text-rose-900',
        info: 'border-indigo-200 bg-indigo-50 text-indigo-900'
    };
    resultBox.className = `mt-2 rounded-lg border p-3 text-xs leading-6 ${toneClasses[tone] || toneClasses.default}`;
    resultBox.innerHTML = html;
}

async function fetchJson(url, options = {}) {
    const response = await window.csrf.fetch(url, {
        headers: { 'Accept': 'application/json', ...(options.headers || {}) },
        ...options
    });

    const contentType = response.headers.get('content-type') || '';
    if (!contentType.includes('application/json')) {
        if (response.redirected && new URL(response.url, window.location.origin).pathname === '/') {
            throw new Error('로그인 세션이 만료됐어요. 다시 로그인한 뒤 목록을 새로고침해 주세요.');
        }
        throw new Error('서버가 JSON 대신 다른 응답을 돌려줬어요. 페이지를 새로고침한 뒤 다시 시도해 주세요.');
    }

    const data = await response.json();
    if (!response.ok) {
        const message = data?.message || data?.detail || '요청 처리에 실패했어요.';
        throw new Error(message);
    }
    return data;
}

async function postJson(url, body) {
    return fetchJson(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });
}

function clearMaskedTimer() {
    if (autoMaskTimer) {
        clearTimeout(autoMaskTimer);
        autoMaskTimer = null;
    }
}

function maskedModeTemplate(data) {
    return `
        <p><span class="font-semibold">아이디:</span> ${data.usernameMasked ?? '-'} </p>
        <p><span class="font-semibold">비밀번호:</span> ${data.passwordMasked ?? '-'} </p>
        ${data.authMethod === 'CERTIFICATE' ? `<p><span class="font-semibold">TrustStore:</span> ${data.trustStorePasswordMasked ?? '-'}</p>` : ''}
        ${data.authMethod === 'CERTIFICATE' ? `<p><span class="font-semibold">KeyStore:</span> ${data.keyStorePasswordMasked ?? '-'}</p>` : ''}
        <p class="mt-2 text-slate-500">기본 조회는 마스킹 상태로 보여줘요. 평문이 필요하면 1회 조회 버튼을 사용해 주세요.</p>
    `;
}

function setDecryptResult(maskedMode, html) {
    renderDecryptResult(html, maskedMode ? 'info' : 'warning');

    if (!maskedMode) {
        clearMaskedTimer();
        return;
    }

    clearMaskedTimer();
    autoMaskTimer = setTimeout(() => {
        state.revealToken = null;
        if (state.lastMaskedResponse) {
            setDecryptResult(true, maskedModeTemplate(state.lastMaskedResponse) + '<p class="mt-2 text-amber-700">30초가 지나 다시 마스킹 상태로 전환했어요. 필요하면 다시 조회해 주세요.</p>');
        }
    }, 30000);
}

async function loadCredentialMasked(mode) {
    if (!state.currentStorageId) {
        throw new Error('먼저 목록에서 저장소를 선택해 주세요.');
    }

    const userKeyInput = document.getElementById('decrypt-user-key');
    const userKey = userKeyInput?.value || '';
    if (!userKey.trim()) {
        // APP_DB_CREDENTIAL_KEY 기반 복호화로 전환되어 별도 사용자 키 없이 진행할 수 있어요.
    }

    let requestBody = {
        userKey,
        revealRaw: mode === 'reveal'
    };

    if (mode === 'reveal' && state.revealToken) {
        requestBody.revealToken = state.revealToken;
    }

    const data = await postJson(`/api/storages/databases/${state.currentStorageId}/decrypt`, requestBody);

    if (mode === 'reveal' && !data.includeRaw && data.tokenIssued && data.revealToken) {
        state.revealToken = data.revealToken;
        const result = await postJson(`/api/storages/databases/${state.currentStorageId}/decrypt`, {
            userKey,
            revealRaw: true,
            revealToken: state.revealToken
        });
        data.password = result.password;
        data.trustStorePassword = result.trustStorePassword;
        data.keyStorePassword = result.keyStorePassword;
        data.username = result.username;
        data.includeRaw = result.includeRaw;
        data.ttlSeconds = result.ttlSeconds;
        state.revealToken = null;
    }

    state.lastMaskedResponse = {
        usernameMasked: data.usernameMasked,
        passwordMasked: data.passwordMasked,
        trustStorePasswordMasked: data.trustStorePasswordMasked,
        keyStorePasswordMasked: data.keyStorePasswordMasked,
        authMethod: data.authMethod
    };

    if (data.includeRaw) {
        const rawHtml = `
            <p class="font-semibold">평문 조회 결과</p>
            <p><span class="font-semibold">아이디:</span> ${data.username ?? data.usernameMasked ?? '-'}</p>
            <p><span class="font-semibold">비밀번호:</span> ${data.password ?? data.passwordMasked ?? '-'}</p>
            ${data.authMethod === 'CERTIFICATE' ? `<p><span class="font-semibold">TrustStore:</span> ${data.trustStorePassword ?? data.trustStorePasswordMasked ?? '-'}</p>` : ''}
            ${data.authMethod === 'CERTIFICATE' ? `<p><span class="font-semibold">KeyStore:</span> ${data.keyStorePassword ?? data.keyStorePasswordMasked ?? '-'}</p>` : ''}
            ${data.keyAlias ? `<p><span class="font-semibold">Key Alias:</span> ${data.keyAlias}</p>` : ''}
            ${data.sslMode ? `<p><span class="font-semibold">SSL Mode:</span> ${data.sslMode}</p>` : ''}
            ${data.ttlSeconds ? `<p class="mt-2 text-amber-700">보안을 위해 평문 재조회는 ${data.ttlSeconds}초 제한이 적용돼요.</p>` : ''}
        `;
        setDecryptResult(false, rawHtml);
        window.storageUi?.showToast({ tone: 'warning', title: '평문을 표시했어요', message: '필요한 확인이 끝나면 바로 화면을 닫거나 다른 항목으로 이동해 주세요.' });
    } else {
        setDecryptResult(true, maskedModeTemplate(data));
        window.storageUi?.showToast({ tone: 'info', title: '마스킹 결과를 불러왔어요', message: '민감한 값은 가린 상태로 보여드렸습니다.', timeout: 2800 });
    }

    state.revealToken = null;
}

document.getElementById('decrypt-mask-btn')?.addEventListener('click', async (event) => {
    event.preventDefault();
    try {
        await loadCredentialMasked('mask');
    } catch (error) {
        renderDecryptResult('복호화 실패: ' + error.message, 'error');
        window.storageUi?.showToast({ tone: 'error', title: '복호화 실패', message: error.message });
    }
});

document.getElementById('decrypt-reveal-btn')?.addEventListener('click', async (event) => {
    event.preventDefault();
    try {
        await loadCredentialMasked('reveal');
    } catch (error) {
        renderDecryptResult('복호화 실패: ' + error.message, 'error');
        window.storageUi?.showToast({ tone: 'error', title: '평문 조회 실패', message: error.message });
    }
});
