let testPassed = false;
let lastTestSummary = null;

function logFormDebug(message, payload) {
    const prefix = '[db-storage-form]';
    if (payload === undefined) {
        console.log(prefix, message);
        return;
    }
    console.log(prefix, message, payload);
}

function showWarning(title, message) {
    window.storageUi?.showToast({ tone: 'warning', title, message, timeout: 4200 });
}

function showError(title, message) {
    window.storageUi?.showToast({ tone: 'error', title, message, timeout: 4600 });
}

function showSuccess(title, message) {
    window.storageUi?.showToast({ tone: 'success', title, message, timeout: 3200 });
}

function setBusy(button, busyLabel) {
    if (!button) {
        return;
    }
    if (!button.dataset.defaultLabel) {
        button.dataset.defaultLabel = button.textContent.trim();
    }
    button.disabled = true;
    button.classList.add('opacity-60');
    button.textContent = busyLabel;
}

function clearBusy(button) {
    if (!button) {
        return;
    }
    button.disabled = false;
    button.classList.remove('opacity-60');
    button.textContent = button.dataset.defaultLabel || button.textContent;
}

function renderInlineResult(tone, title, message) {
    const result = document.getElementById('result');
    if (!result) {
        return;
    }

    const styles = {
        success: 'border-emerald-200 bg-emerald-50 text-emerald-900',
        warning: 'border-amber-200 bg-amber-50 text-amber-900',
        error: 'border-rose-200 bg-rose-50 text-rose-900',
        info: 'border-indigo-200 bg-indigo-50 text-indigo-900'
    };
    const icons = {
        success: '✅',
        warning: '⚠️',
        error: '⛔',
        info: 'ℹ️'
    };

    result.className = `mt-5 rounded-2xl border p-4 text-sm leading-6 sm:mt-6 ${styles[tone] || styles.info}`;
    result.innerHTML = `
        <div class="flex items-start gap-3">
            <span class="mt-0.5 text-lg">${icons[tone] || icons.info}</span>
            <div>
                <p class="font-semibold">${title}</p>
                <p class="mt-1 whitespace-pre-wrap">${message}</p>
            </div>
        </div>
    `;
}

function syncOracleSchemaWithUsername() {
    const vendor = document.getElementById('vendor')?.value;
    const username = document.getElementById('username');
    const schemaInput = document.getElementById('schemaName');
    if (vendor !== 'ORACLE' || !username || !schemaInput) {
        return;
    }
    schemaInput.value = (username.value || '').trim().toUpperCase();
}

function updateDbFieldLabels() {
    const vendor = document.getElementById('vendor')?.value;
    const schemaLabel = document.getElementById('schemaLabel');
    const schemaInput = document.getElementById('schemaName');
    const databaseInput = document.getElementById('databaseName');
    const serviceNameField = document.getElementById('serviceNameField');
    const databaseNameField = document.getElementById('databaseNameField');
    const postgresSchemaHint = document.getElementById('postgresSchemaHint');
    const oracleSchemaHelp = document.getElementById('oracleSchemaHelp');

    if (!schemaLabel || !schemaInput) {
        return;
    }

    const isOracle = vendor === 'ORACLE';
    const isPostgresql = vendor === 'POSTGRESQL';
    const isMySqlFamily = vendor === 'MYSQL' || vendor === 'MARIADB';
    const isRoutingDb = isPostgresql;

    if (serviceNameField) {
        serviceNameField.classList.toggle('hidden', !isOracle && !isPostgresql);
    }
    if (databaseNameField) {
        databaseNameField.classList.toggle('hidden', !isRoutingDb);
    }
    if (postgresSchemaHint) {
        postgresSchemaHint.classList.toggle('hidden', vendor !== 'POSTGRESQL');
    }

    if (databaseInput) {
        databaseInput.value = isPostgresql ? 'etl_sfdc' : '';
        databaseInput.disabled = isPostgresql;
        databaseInput.classList.toggle('bg-slate-50', isPostgresql);
        databaseInput.classList.toggle('text-slate-500', isPostgresql);
        databaseInput.classList.toggle('cursor-not-allowed', isPostgresql);
    }

    if (isOracle) {
        schemaLabel.textContent = 'Oracle 스키마(사용자명 연동)';
        schemaInput.placeholder = '사용자명을 입력하면 자동으로 맞춰져요';
        schemaInput.readOnly = false;
        schemaInput.disabled = true;
        schemaInput.classList.add('bg-slate-50', 'text-slate-500', 'cursor-not-allowed');
        oracleSchemaHelp?.classList.remove('hidden');
        syncOracleSchemaWithUsername();
    } else if (isPostgresql) {
        schemaLabel.textContent = '라우팅 스키마명';
        schemaInput.placeholder = 'PostgreSQL은 고정 스키마 규칙을 사용해요';
        schemaInput.readOnly = false;
        schemaInput.disabled = true;
        schemaInput.classList.add('bg-slate-50', 'text-slate-500', 'cursor-not-allowed');
        oracleSchemaHelp?.classList.add('hidden');
        schemaInput.value = 'org_<org명>';
    } else if (isMySqlFamily) {
        schemaLabel.textContent = '스키마 정보';
        schemaInput.placeholder = 'MySQL/MariaDB는 스키마 입력 없이 연결만 확인해요';
        schemaInput.readOnly = false;
        schemaInput.disabled = false;
        schemaInput.classList.remove('bg-slate-50', 'text-slate-500', 'cursor-not-allowed');
        oracleSchemaHelp?.classList.add('hidden');
        schemaInput.value = '';
    } else {
        schemaInput.readOnly = false;
        schemaInput.disabled = false;
        schemaInput.classList.remove('bg-slate-50', 'text-slate-500', 'cursor-not-allowed');
        oracleSchemaHelp?.classList.add('hidden');
        schemaInput.value = '';
    }
}

function toggleAuthFields() {
    const authMethod = document.getElementById('authMethod')?.value;
    const passwordFields = document.getElementById('password-auth-fields');
    const certificateFields = document.getElementById('certificate-auth-fields');

    if (!passwordFields || !certificateFields) {
        return;
    }

    if (authMethod === 'CERTIFICATE') {
        passwordFields.classList.add('hidden');
        certificateFields.classList.remove('hidden');
    } else {
        passwordFields.classList.remove('hidden');
        certificateFields.classList.add('hidden');
    }

    logFormDebug('toggleAuthFields', { authMethod });
}

function ensureVendorSelected(report = true) {
    const vendor = document.getElementById('vendor');
    if (!vendor) {
        logFormDebug('vendor field missing');
        return true;
    }
    if (vendor.value) {
        vendor.setCustomValidity('');
        logFormDebug('vendor valid', { value: vendor.value });
        return true;
    }
    vendor.setCustomValidity('DB 종류를 먼저 선택해 주세요.');
    logFormDebug('vendor missing, blocking submit');
    if (report) {
        vendor.reportValidity();
        showWarning('DB 종류를 먼저 선택해 주세요', 'DB 종류를 고른 뒤에 연결 테스트나 등록을 진행할 수 있어요.');
    }
    return false;
}

function ensureRequiredFields(report = true) {
    const vendor = document.getElementById('vendor');
    const name = document.getElementById('name');
    const url = document.getElementById('jdbcUrl');
    const port = document.getElementById('port');
    const databaseName = document.getElementById('databaseName');
    const username = document.getElementById('username');

    if (!ensureVendorSelected(report)) {
        return false;
    }

    const requiredTextFields = [
        { el: name, label: '저장소명', message: '저장소 이름을 입력해 주세요.' },
        { el: url, label: 'DB URL', message: 'DB URL을 입력해 주세요.' },
        { el: port, label: '포트', message: '포트를 입력해 주세요.' },
        { el: username, label: '사용자명', message: '사용자명을 입력해 주세요.' }
    ];

    for (const item of requiredTextFields) {
        if (!item.el || item.el.value === null || item.el.value.trim() === '') {
            if (item.el) {
                item.el.setCustomValidity(item.message);
                logFormDebug('required field missing', { label: item.label, id: item.el.id });
                if (report) {
                    item.el.reportValidity();
                    showWarning(`${item.label} 입력이 필요해요`, item.message);
                    item.el.focus();
                }
            }
            return false;
        }
        item.el.setCustomValidity('');
    }

    if (vendor?.value === 'POSTGRESQL') {
        if (!databaseName || databaseName.value == null || databaseName.value.trim() === '') {
            if (databaseName) {
                const message = '데이터베이스명은 PostgreSQL에서 필수입니다.';
                databaseName.setCustomValidity(message);
                logFormDebug('required field missing', { label: 'databaseName', id: 'databaseName', vendor: vendor.value });
                if (report) {
                    databaseName.reportValidity();
                    showWarning('데이터베이스명을 입력해 주세요', message);
                    databaseName.focus();
                }
            }
            return false;
        }
        databaseName.setCustomValidity('');
    } else if (databaseName) {
        databaseName.setCustomValidity('');
    }

    return true;
}

function collectFieldSnapshot(form) {
    const data = new FormData(form);
    const snapshot = {};
    for (const [key, value] of data.entries()) {
        snapshot[key] = key.toLowerCase().includes('password') || key.toLowerCase().includes('key')
            ? '****'
            : value;
    }
    return snapshot;
}

function collectPayload() {
    return {
        name: document.getElementById('name')?.value?.trim() || '',
        description: document.getElementById('description')?.value?.trim() || '',
        vendor: document.getElementById('vendor')?.value || '',
        authMethod: document.getElementById('authMethod')?.value || '',
        schemaName: document.getElementById('schemaName')?.value?.trim() || '',
        databaseName: document.getElementById('databaseName')?.value?.trim() || '',
        url: document.getElementById('jdbcUrl')?.value?.trim() || '',
        port: Number(document.getElementById('port')?.value || 0) || null,
        username: document.getElementById('username')?.value?.trim() || '',
        password: document.getElementById('password')?.value || '',
        certificateAuth: {
            trustStorePath: document.getElementById('trustStorePath')?.value?.trim() || '',
            trustStorePassword: document.getElementById('trustStorePassword')?.value || '',
            keyStorePath: document.getElementById('keyStorePath')?.value?.trim() || '',
            keyStorePassword: document.getElementById('keyStorePassword')?.value || '',
            keyAlias: document.getElementById('keyAlias')?.value?.trim() || '',
            sslMode: document.getElementById('sslMode')?.value?.trim() || ''
        }
    };
}

async function postJson(url, payload) {
    const response = await window.csrf.fetch(url, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        },
        body: JSON.stringify(payload)
    });

    const contentType = response.headers.get('content-type') || '';
    if (!contentType.includes('application/json')) {
        if (response.redirected && new URL(response.url, window.location.origin).pathname === '/') {
            throw new Error('로그인 세션이 만료됐어요. 다시 로그인한 뒤 이 페이지로 돌아와 주세요.');
        }
        throw new Error('서버가 JSON 대신 다른 응답을 돌려줬어요. 새로고침 후 다시 시도해 주세요.');
    }

    const data = await response.json();
    if (!response.ok) {
        throw new Error(data?.message || data?.detail || '요청 처리에 실패했어요.');
    }
    return data;
}

function logInvalidControls(form) {
    const invalidControls = Array.from(form.querySelectorAll(':invalid')).map((element) => ({
        id: element.id,
        name: element.name,
        type: element.type,
        value: element.type === 'password' ? '****' : element.value,
        validationMessage: element.validationMessage
    }));
    logFormDebug('invalid controls', invalidControls);
}

function bindFormDebugging() {
    const form = document.getElementById('db-storage-form');
    const testButton = document.getElementById('test-button');
    const registerButton = document.getElementById('register-button');

    if (!form) {
        logFormDebug('form not found');
        return;
    }

    logFormDebug('form ready', {
        action: form.action,
        method: form.method,
        fields: collectFieldSnapshot(form)
    });

    form.addEventListener('invalid', (event) => {
        logFormDebug('invalid event fired', {
            targetId: event.target?.id,
            targetName: event.target?.name,
            validationMessage: event.target?.validationMessage
        });
    }, true);

    form.addEventListener('submit', (event) => {
        const submitter = event.submitter || document.activeElement;
        const targetAction = submitter?.getAttribute?.('formaction') || form.action;
        logFormDebug('submit event fired', {
            submitterId: submitter?.id,
            submitterText: submitter?.textContent?.trim(),
            formAction: form.action,
            targetAction,
            method: form.method,
            checkValidity: form.checkValidity(),
            fields: collectFieldSnapshot(form)
        });

        if (submitter?.id === 'test-button') {
            event.preventDefault();
            return;
        }

        if (!ensureRequiredFields(true)) {
            event.preventDefault();
            logFormDebug('submit prevented: required field missing');
            return;
        }

        if (!form.checkValidity()) {
            logFormDebug('submit blocked by browser validation');
            logInvalidControls(form);
            event.preventDefault();
            return;
        }

        if (submitter?.id === 'register-button') {
            setBusy(registerButton, '등록 중...');
        }

        logFormDebug('submit proceeding', {
            submitterId: submitter?.id,
            targetAction
        });
    });

    testButton?.addEventListener('click', async (event) => {
        logFormDebug('test button clicked', {
            testUrl: testButton.dataset.testUrl,
            type: testButton.type,
            vendor: document.getElementById('vendor')?.value
        });

        if (!ensureRequiredFields(true)) {
            event.preventDefault();
            event.stopPropagation();
            logFormDebug('test click blocked before ajax');
            return;
        }

        const payload = collectPayload();
        setBusy(testButton, '테스트 중...');
        try {
            logFormDebug('ajax test request', payload);
            const data = await postJson(testButton.dataset.testUrl, payload);
            testPassed = !!data.success;
            lastTestSummary = data.message || '';
            renderInlineResult(data.success ? 'success' : 'warning', data.success ? '연결 테스트 성공' : '연결 테스트 결과 확인 필요', data.message || '연결 테스트가 끝났어요.');
            if (data.success) {
                showSuccess('연결 테스트 성공', data.message || '연결 테스트가 끝났어요.');
            } else {
                showWarning('연결 테스트 결과 확인', data.message || '연결 테스트가 끝났어요.');
            }
            logFormDebug('ajax test success', { success: data.success, message: data.message });
        } catch (error) {
            testPassed = false;
            lastTestSummary = null;
            renderInlineResult('error', '연결 테스트 실패', error.message);
            showError('연결 테스트 실패', error.message);
            logFormDebug('ajax test failed', { message: error.message });
        } finally {
            clearBusy(testButton);
        }
    }, true);

    registerButton?.addEventListener('click', (event) => {
        logFormDebug('register button clicked', {
            formaction: registerButton.getAttribute('formaction'),
            type: registerButton.type,
            vendor: document.getElementById('vendor')?.value,
            testPassed,
            lastTestSummary
        });
        if (!ensureVendorSelected(true)) {
            event.preventDefault();
            event.stopPropagation();
            logFormDebug('register click blocked before submit');
        }
    }, true);
}

document.getElementById('vendor')?.addEventListener('change', (event) => {
    event.target.setCustomValidity('');
    logFormDebug('vendor changed', { value: event.target.value });
    updateDbFieldLabels();
});
document.getElementById('authMethod')?.addEventListener('change', (event) => {
    logFormDebug('authMethod changed', { value: event.target.value });
    toggleAuthFields();
});
document.getElementById('username')?.addEventListener('input', () => {
    syncOracleSchemaWithUsername();
});

document.getElementById('databaseName')?.addEventListener('input', (event) => {
    event.target.setCustomValidity('');
});

window.addEventListener('error', (event) => {
    logFormDebug('window error', {
        message: event.message,
        source: event.filename,
        line: event.lineno,
        column: event.colno
    });
});

window.addEventListener('unhandledrejection', (event) => {
    logFormDebug('unhandled rejection', {
        reason: event.reason?.message || String(event.reason)
    });
});

bindFormDebugging();
updateDbFieldLabels();
toggleAuthFields();
