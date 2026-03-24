(function (window, document) {
    const DEFAULT_HEADER = 'X-CSRF-TOKEN';
    const LOGIN_PATHS = new Set(['/', '/user/login']);

    const state = {
        parameterName: readMeta('csrf-parameter'),
        headerName: readMeta('csrf-header') || DEFAULT_HEADER,
        token: readMeta('csrf-token') || null,
        cookieToken: readCookie('XSRF-TOKEN') || null
    };

    function debug(message, payload) {
        const prefix = '[csrf-fetch]';
        if (payload === undefined) {
            console.log(prefix, message);
            return;
        }
        console.log(prefix, message, payload);
    }

    function readMeta(name) {
        const element = document.querySelector(`meta[name="${name}"]`);
        return element ? element.getAttribute('content') : null;
    }

    function upsertMeta(name, value) {
        let element = document.querySelector(`meta[name="${name}"]`);
        if (!element) {
            element = document.createElement('meta');
            element.setAttribute('name', name);
            document.head.appendChild(element);
        }
        element.setAttribute('content', value || '');
    }

    function readCookie(name) {
        const encodedName = `${encodeURIComponent(name)}=`;
        const parts = document.cookie ? document.cookie.split('; ') : [];
        for (const part of parts) {
            if (part.startsWith(encodedName)) {
                return decodeURIComponent(part.substring(encodedName.length));
            }
        }
        return null;
    }

    function syncToken(token, headerName, parameterName) {
        state.token = token || null;
        state.headerName = headerName || state.headerName || DEFAULT_HEADER;
        state.parameterName = parameterName || state.parameterName || null;
        state.cookieToken = readCookie('XSRF-TOKEN') || state.cookieToken || null;

        upsertMeta('csrf-token', state.token || '');
        upsertMeta('csrf-header', state.headerName || DEFAULT_HEADER);
        if (state.parameterName) {
            upsertMeta('csrf-parameter', state.parameterName);
        }

        debug('syncToken', {
            headerName: state.headerName,
            hasMetaToken: !!state.token,
            hasCookieToken: !!state.cookieToken,
            metaEqualsCookie: !!state.token && !!state.cookieToken && state.token === state.cookieToken
        });
    }

    function isMutating(method) {
        const normalized = (method || 'GET').toUpperCase();
        return !['GET', 'HEAD', 'OPTIONS', 'TRACE'].includes(normalized);
    }

    function sameOrigin(url) {
        const resolved = new URL(url, window.location.origin);
        return resolved.origin === window.location.origin;
    }

    function isLoginRedirect(response) {
        if (!response.redirected || !response.url) {
            return false;
        }
        const target = new URL(response.url, window.location.origin);
        return target.origin === window.location.origin && LOGIN_PATHS.has(target.pathname);
    }

    async function parseTokenResponse(response) {
        if (isLoginRedirect(response)) {
            throw new Error('로그인 세션이 만료되었어요. 다시 로그인한 뒤 새로고침해 주세요.');
        }

        const contentType = response.headers.get('content-type') || '';
        if (!response.ok || !contentType.includes('application/json')) {
            throw new Error('CSRF 토큰을 새로 가져오지 못했어요. 페이지를 새로고침해 주세요.');
        }

        return response.json();
    }

    async function refreshToken() {
        debug('refreshToken:start');
        const response = await fetch('/api/csrf', {
            method: 'GET',
            credentials: 'same-origin',
            headers: {
                'Accept': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            }
        });

        const data = await parseTokenResponse(response);
        syncToken(data.token, data.headerName, data.parameterName);
        debug('refreshToken:done', {
            headerName: data.headerName,
            parameterName: data.parameterName,
            hasToken: !!data.token
        });
        return state.token;
    }

    async function ensureTokenForRequest() {
        state.cookieToken = readCookie('XSRF-TOKEN') || state.cookieToken || null;
        if (!state.token) {
            debug('ensureTokenForRequest:no-meta-token-refreshing', {
                hasCookieToken: !!state.cookieToken
            });
            await refreshToken();
        }
    }

    async function applyCsrfHeaders(url, options) {
        const requestOptions = {
            credentials: 'same-origin',
            redirect: 'follow',
            ...options
        };
        const headers = new Headers(requestOptions.headers || {});
        const method = requestOptions.method || 'GET';

        if (sameOrigin(url) && isMutating(method)) {
            await ensureTokenForRequest();
            if (state.token) {
                headers.set(state.headerName || DEFAULT_HEADER, state.token);
            }
            headers.set('X-Requested-With', 'XMLHttpRequest');
            debug('applyCsrfHeaders', {
                url,
                method,
                headerName: state.headerName || DEFAULT_HEADER,
                hasMetaToken: !!state.token,
                hasCookieToken: !!state.cookieToken,
                metaEqualsCookie: !!state.token && !!state.cookieToken && state.token === state.cookieToken
            });
        }

        requestOptions.headers = headers;
        return requestOptions;
    }

    async function csrfFetch(url, options = {}, attempt = 0) {
        const requestOptions = await applyCsrfHeaders(url, options);
        let response = await fetch(url, requestOptions);

        if (isLoginRedirect(response)) {
            const error = new Error('로그인 세션이 만료되었어요. 다시 로그인한 뒤 새로고침해 주세요.');
            error.code = 'AUTH_REQUIRED';
            throw error;
        }

        if (response.status === 403 && attempt === 0 && sameOrigin(url) && isMutating(requestOptions.method)) {
            debug('csrfFetch:403-retrying', { url, method: requestOptions.method });
            await refreshToken();
            response = await fetch(url, await applyCsrfHeaders(url, options));
        }

        if (isLoginRedirect(response)) {
            const error = new Error('로그인 세션이 만료되었어요. 다시 로그인한 뒤 새로고침해 주세요.');
            error.code = 'AUTH_REQUIRED';
            throw error;
        }

        return response;
    }

    syncToken(state.token, state.headerName, state.parameterName);

    window.csrf = {
        fetch: csrfFetch,
        refreshToken,
        getToken: function () {
            return state.token;
        },
        getHeaderName: function () {
            return state.headerName || DEFAULT_HEADER;
        }
    };
})(window, document);
