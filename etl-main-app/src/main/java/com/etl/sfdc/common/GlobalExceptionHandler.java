package com.etl.sfdc.common;

import com.etlplatform.common.error.ApiErrorResponse;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.error.FeatureDisabledException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(FeatureDisabledException.class)
    public ResponseEntity<?> handleFeatureDisabled(FeatureDisabledException ex, HttpServletRequest request) {
        log.warn("Feature disabled: {}", ex.getMessage());
        if (wantsHtml(request)) {
            return htmlError(HttpStatus.SERVICE_UNAVAILABLE, "FEATURE_DISABLED", "기능을 지금은 사용할 수 없어요", ex.getMessage(), "/");
        }
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ApiErrorResponse("FEATURE_DISABLED", "Feature is unavailable in current mode", ex.getMessage()));
    }

    @ExceptionHandler(AppException.class)
    public ResponseEntity<?> handleAppException(AppException ex, HttpServletRequest request) {
        log.warn("Application exception: {}", ex.getMessage(), ex);
        if (wantsHtml(request)) {
            return htmlError(HttpStatus.BAD_REQUEST, "BAD_REQUEST", "요청을 처리하지 못했어요", ex.getMessage(), "/etl/objects");
        }
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ApiErrorResponse("BAD_REQUEST", "Invalid request", ex.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> handleException(Exception ex, HttpServletRequest request) {
        log.error("Unhandled exception", ex);
        if (wantsHtml(request)) {
            return htmlError(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_SERVER_ERROR", "예상하지 못한 오류가 발생했어요", ex.getMessage() == null ? "서버 로그를 확인해 주세요." : ex.getMessage(), "/");
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiErrorResponse("INTERNAL_SERVER_ERROR", "Unexpected error", ex.getMessage()));
    }

    private boolean wantsHtml(HttpServletRequest request) {
        String accept = request.getHeader(HttpHeaders.ACCEPT);
        return accept != null && accept.contains(MediaType.TEXT_HTML_VALUE);
    }

    private ResponseEntity<String> htmlError(HttpStatus status, String code, String title, String message, String retryUrl) {
        String safeCode = escapeHtml(code);
        String safeTitle = escapeHtml(title);
        String safeMessage = escapeHtml(message == null ? "오류 상세 메시지가 없습니다." : message);
        String safeRetryUrl = escapeHtml(retryUrl == null || retryUrl.isBlank() ? "/" : retryUrl);
        String html = """
                <!DOCTYPE html>
                <html lang=\"ko\">
                <head>
                    <meta charset=\"UTF-8\">
                    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
                    <title>ETL 오류</title>
                    <script src=\"https://cdn.tailwindcss.com\"></script>
                </head>
                <body class=\"min-h-screen bg-gradient-to-br from-indigo-50 via-white to-violet-100 text-slate-800\">
                <div class=\"mx-auto flex min-h-screen w-full max-w-5xl items-center justify-center px-4 py-8 sm:px-6 lg:px-8\">
                    <main class=\"w-full max-w-2xl rounded-[28px] border border-rose-100 bg-white/95 p-6 shadow-xl shadow-indigo-100 sm:p-8\">
                        <div class=\"flex items-start gap-3\">
                            <span class=\"inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-2xl bg-rose-100 text-xl\">⚠️</span>
                            <div class=\"min-w-0\">
                                <div class=\"flex flex-wrap items-center gap-2\">
                                    <p class=\"text-sm font-semibold uppercase tracking-wide text-rose-500\">ETL Error</p>
                                    <span class=\"inline-flex items-center rounded-full bg-indigo-100 px-2.5 py-1 text-[11px] font-semibold text-indigo-700\">%s</span>
                                    <span class=\"inline-flex items-center rounded-full bg-slate-100 px-2.5 py-1 text-[11px] font-semibold text-slate-600\">HTTP %s</span>
                                </div>
                                <h1 class=\"mt-2 text-2xl font-bold tracking-tight text-rose-700 sm:text-3xl\">%s</h1>
                                <p class=\"mt-3 text-sm leading-6 text-slate-500\">브라우저에서 바로 이해하기 쉽게 표시한 오류 화면입니다. 자세한 원인은 서버 쉘 로그에도 함께 남겨뒀어요.</p>
                            </div>
                        </div>

                        <section class=\"mt-6 rounded-2xl border border-rose-200 bg-rose-50 p-4 sm:p-5\">
                            <p class=\"text-[11px] font-semibold uppercase tracking-wide text-rose-500\">사용자용 안내</p>
                            <p class=\"mt-2 text-sm leading-6 text-slate-700\">현재 요청을 처리하지 못했어요. 설정값, 로그인 상태, 또는 외부 연동 상태를 한 번 확인해 주세요.</p>
                        </section>

                        <details class=\"mt-4 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm group\">
                            <summary class=\"cursor-pointer list-none text-sm font-semibold text-slate-700\">개발자용 상세 보기</summary>
                            <div class=\"mt-3 rounded-xl bg-slate-50 p-4\">
                                <p class=\"text-[11px] font-semibold uppercase tracking-wide text-slate-500\">상세 메시지</p>
                                <p class=\"mt-2 whitespace-pre-wrap break-words text-sm leading-6 text-slate-700\">%s</p>
                            </div>
                        </details>

                        <div class=\"mt-6 flex flex-col gap-3 sm:flex-row\">
                            <a href=\"%s\" class=\"inline-flex items-center justify-center rounded-xl bg-gradient-to-r from-indigo-600 to-violet-600 px-5 py-3 text-sm font-semibold text-white transition hover:from-indigo-700 hover:to-violet-700\">다시 시도하기</a>
                            <a href=\"/\" class=\"inline-flex items-center justify-center rounded-xl border border-slate-200 bg-white px-5 py-3 text-sm font-semibold text-slate-700 transition hover:bg-slate-50\">홈으로 돌아가기</a>
                            <a href=\"javascript:history.back()\" class=\"inline-flex items-center justify-center rounded-xl border border-slate-200 bg-white px-5 py-3 text-sm font-semibold text-slate-700 transition hover:bg-slate-50\">이전 페이지</a>
                        </div>
                    </main>
                </div>
                </body>
                </html>
                """.formatted(safeCode, status.value(), safeTitle, safeMessage, safeRetryUrl);

        return ResponseEntity.status(status)
                .contentType(MediaType.TEXT_HTML)
                .body(html);
    }

    private String escapeHtml(String value) {
        return value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#39;");
    }
}
