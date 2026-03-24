const params = new URLSearchParams(window.location.search);
const msg = params.get('message');
if (msg === 'token_refreshed') {
    document.getElementById('result').innerText = '✅ 토큰이 갱신되었습니다.';

    const url = new URL(window.location);
    url.searchParams.delete('message');
    window.history.replaceState({}, '', url);
}

let startTime = null;

function upload() {
    startTime = new Date();
    const resultDiv = document.getElementById('result');
    resultDiv.innerHTML = "";

    const directory = document.getElementById("upload-directory").value.trim();
    const uploadParams = new URLSearchParams();
    if (directory) {
        uploadParams.set('directory', directory);
    }

    const source = new EventSource(`/upload?${uploadParams.toString()}`);

    source.onmessage = function(event) {
        if (event.data.startsWith("progress:")) {
            const progressPayload = event.data.split(":")[1];
            const [percentStr, processedStr, totalStr] = progressPayload.split(",");

            const percent = parseFloat(percentStr);
            const processed = parseInt(processedStr);
            const total = parseInt(totalStr);

            updateProgressBar(percent, processed, total);
        } else {
            appendLine(event.data);
        }
    };

    source.addEventListener("summary", function(event) {
        const summary = JSON.parse(event.data);
        appendLine(`🧾 JSON 이력 저장: ${summary.historyFile}`);
        appendLine(`📦 성공 ${summary.successCount}건 / 실패 ${summary.failureCount}건`);
    });

    source.onerror = function() {
        source.close();
        appendLine('❌ 연결 종료');
    };

    function appendLine(text) {
        const line = document.createElement('div');
        line.textContent = text;
        resultDiv.appendChild(line);
        resultDiv.scrollTop = resultDiv.scrollHeight;

        const MAX_LINES = 1000;
        while (resultDiv.children.length > MAX_LINES) {
            resultDiv.removeChild(resultDiv.firstChild);
        }

        if (text.includes("🎉 전체 완료") || text.includes("✅ 업로드할 파일이 없습니다")) {
            const endTime = new Date();
            const totalSeconds = Math.round((endTime - startTime) / 1000);
            const formatted = formatSeconds(totalSeconds);
            const timeDiv = document.createElement('div');
            timeDiv.textContent = `⏱ 총 소요 시간: ${formatted}`;
            resultDiv.appendChild(timeDiv);
        }
    }
}

function updateProgressBar(percent, processed, total) {
    const bar = document.getElementById("progress-bar");
    const label = document.getElementById("progress-label");
    const count = document.getElementById("progress-count");

    bar.style.width = percent + "%";
    label.textContent = "진행률: " + percent.toFixed(1) + "%";
    count.textContent = processed + "건 / " + total + "건";

    if (startTime && processed > 0) {
        const elapsedSec = (new Date() - startTime) / 1000;
        const avgPerItem = elapsedSec / processed;
        const remaining = total - processed;
        const remainingSec = Math.round(avgPerItem * remaining);
        const formatted = formatSeconds(remainingSec);

        const etaDiv = document.getElementById("eta-time");
        etaDiv.textContent = "⏳ 예상 남은 시간: " + formatted;
    }
}

function formatSeconds(seconds) {
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    const hh = String(h).padStart(2, '0');
    const mm = String(m).padStart(2, '0');
    const ss = String(s).padStart(2, '0');
    return `${hh}:${mm}:${ss}`;
}
