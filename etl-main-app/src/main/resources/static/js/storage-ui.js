(function () {
    function ensureToastRoot() {
        let root = document.getElementById('storage-toast-root');
        if (root) {
            return root;
        }
        root = document.createElement('div');
        root.id = 'storage-toast-root';
        root.className = 'pointer-events-none fixed right-4 top-4 z-50 flex w-[min(92vw,26rem)] flex-col gap-3';
        document.body.appendChild(root);
        return root;
    }

    function toneClasses(tone) {
        switch (tone) {
            case 'success':
                return 'border-emerald-200 bg-emerald-50 text-emerald-900';
            case 'warning':
                return 'border-amber-200 bg-amber-50 text-amber-900';
            case 'error':
                return 'border-rose-200 bg-rose-50 text-rose-900';
            default:
                return 'border-indigo-200 bg-white text-slate-900';
        }
    }

    function toneIcon(tone) {
        switch (tone) {
            case 'success':
                return '✅';
            case 'warning':
                return '⚠️';
            case 'error':
                return '⛔';
            default:
                return 'ℹ️';
        }
    }

    function escapeHtml(value) {
        return String(value ?? '')
            .replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#39;');
    }

    function showToast(options) {
        const root = ensureToastRoot();
        const tone = options?.tone || 'info';
        const title = options?.title || '안내';
        const message = options?.message || '';
        const timeout = Number(options?.timeout ?? 4200);

        const toast = document.createElement('div');
        toast.className = `pointer-events-auto overflow-hidden rounded-2xl border px-4 py-3 shadow-xl shadow-slate-200/60 backdrop-blur ${toneClasses(tone)}`;
        toast.innerHTML = `
            <div class="flex items-start gap-3">
                <span class="mt-0.5 text-lg">${toneIcon(tone)}</span>
                <div class="min-w-0 flex-1">
                    <p class="text-sm font-semibold">${escapeHtml(title)}</p>
                    <p class="mt-1 text-sm leading-6 opacity-90">${escapeHtml(message)}</p>
                </div>
                <button type="button" class="rounded-lg px-2 py-1 text-xs font-semibold opacity-70 transition hover:opacity-100">닫기</button>
            </div>
        `;

        const close = () => {
            toast.classList.add('opacity-0', 'translate-y-1');
            setTimeout(() => toast.remove(), 180);
        };

        toast.querySelector('button')?.addEventListener('click', close);
        root.appendChild(toast);

        if (timeout > 0) {
            setTimeout(close, timeout);
        }
    }

    window.storageUi = {
        showToast
    };
})();
