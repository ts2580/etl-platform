package com.sfdcupload.file.controller;

import com.etlplatform.common.error.AppException;
import com.etlplatform.common.validation.RequestValidationUtils;
import com.sfdcupload.common.SalesforceTokenManager;
import com.sfdcupload.file.service.FileMigrationService;
import com.sfdcupload.file.service.FileService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@RestController
@RequiredArgsConstructor
@Slf4j
public class FileController {

    private final FileService fileService;
    private final SalesforceTokenManager tokenManager;
    private final FileMigrationService fileMigrationService;

    private static final Path EFS_ROOT = Paths.get("/mnt/efs/sfdc");

    @GetMapping("/ping")
    public String ping() {
        return "pong!";
    }

    @PutMapping("/uploadEFS")
    public ResponseEntity<String> uploadFile(HttpServletRequest request,
                                            @RequestHeader("X-Filename") String encodedFileName) {
        try {
            Files.createDirectories(EFS_ROOT);
            String fileName = URLDecoder.decode(encodedFileName, StandardCharsets.UTF_8);
            String safeFileName = Paths.get(fileName).getFileName().toString();
            Path targetPath = EFS_ROOT.resolve(safeFileName);

            log.info("Uploading file to EFS. fileName={}, targetPath={}", safeFileName, targetPath);
            try (InputStream in = request.getInputStream();
                 OutputStream out = Files.newOutputStream(targetPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                byte[] buffer = new byte[8192];
                int len;
                while ((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
            }
            return ResponseEntity.ok("OK");
        } catch (IOException e) {
            log.error("uploadEFS 처리 중 오류 발생", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("File upload failed.");
        }
    }

    @GetMapping("/upload")
    public SseEmitter upload(@RequestParam String dataId, @RequestParam int cycle, HttpSession session) {
        String validatedDataId = RequestValidationUtils.requireSimpleKey(dataId, "dataId");
        int validatedCycle = RequestValidationUtils.requirePositive(cycle, "cycle");
        SseEmitter emitter = new SseEmitter(0L);
        String accessToken = tokenManager.getAccessToken(session);

        if (accessToken == null) {
            throw new AppException("로그인이 필요합니다. 먼저 OAuth 로그인을 진행해 주세요.");
        }

        log.info("Starting file upload migration. dataId={}, cycle={}", validatedDataId, validatedCycle);
        new Thread(() -> {
            try {
                fileMigrationService.migrate(validatedDataId, validatedCycle, accessToken, emitter);
            } catch (Exception e) {
                log.error("업로드 진행 중 예외 발생. dataId={}", validatedDataId, e);
                try {
                    emitter.send(SseEmitter.event().data("❌ 처리 중 예외 발생 : " + e.getMessage()));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            } finally {
                emitter.complete();
            }
        }).start();

        return emitter;
    }
}
