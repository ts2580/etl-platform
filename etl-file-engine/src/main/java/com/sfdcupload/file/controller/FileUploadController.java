package com.sfdcupload.file.controller;

import com.etlplatform.common.error.AppException;
import com.sfdcupload.common.SalesforceOrgCredential;
import com.sfdcupload.common.SalesforceOrgService;
import com.sfdcupload.file.application.upload.FileUploadService;
import com.sfdcupload.file.domain.result.FileUploadSummary;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
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
public class FileUploadController {
    private static final Path DEFAULT_UPLOAD_ROOT = Paths.get("/mnt/efs/sfdc");

    private final SalesforceOrgService salesforceOrgService;
    private final FileUploadService fileUploadService;

    @GetMapping("/ping")
    public String ping() {
        return "pong!";
    }

    @PutMapping("/uploadEFS")
    public ResponseEntity<String> uploadFile(HttpServletRequest request,
                                             @RequestHeader("X-Filename") String encodedFileName) {
        try {
            Files.createDirectories(DEFAULT_UPLOAD_ROOT);
            String fileName = URLDecoder.decode(encodedFileName, StandardCharsets.UTF_8);
            String safeFileName = Paths.get(fileName).getFileName().toString();
            Path targetPath = DEFAULT_UPLOAD_ROOT.resolve(safeFileName);

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
    public SseEmitter upload(@RequestParam(required = false) String directory) {
        SseEmitter emitter = new SseEmitter(0L);
        Path uploadRoot = resolveUploadRoot(directory);

        SalesforceOrgCredential activeOrg = salesforceOrgService.resolveSelectedOrg(null);
        if (activeOrg == null) {
            throw new AppException("사용할 Salesforce org가 없습니다. 메인 모듈에서 org를 먼저 선택해 주세요.");
        }

        String accessToken = activeOrg.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) {
            throw new AppException("파일 모듈에서 사용할 Salesforce access token이 없어요. 메인 모듈에서 토큰을 먼저 받아와 주세요.");
        }

        String myDomain = activeOrg.getMyDomain();
        if (myDomain == null || myDomain.isBlank()) {
            throw new AppException("선택한 org의 myDomain 정보가 비어 있어요.");
        }

        log.info("Starting generic file upload. orgKey={}, rootPath={}", activeOrg.getOrgKey(), uploadRoot);
        new Thread(() -> {
            try {
                FileUploadSummary summary = fileUploadService.uploadDirectory(uploadRoot, accessToken, myDomain, emitter);
                emitter.send(SseEmitter.event().name("summary").data(summary));
            } catch (Exception e) {
                log.error("업로드 진행 중 예외 발생", e);
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

    @GetMapping("/upload/result")
    public FileUploadSummary uploadResult(@RequestParam(required = false) String directory) {
        Path uploadRoot = resolveUploadRoot(directory);
        SalesforceOrgCredential activeOrg = salesforceOrgService.resolveSelectedOrg(null);
        if (activeOrg == null) {
            throw new AppException("사용할 Salesforce org가 없습니다. 메인 모듈에서 org를 먼저 선택해 주세요.");
        }
        return fileUploadService.uploadDirectory(uploadRoot, activeOrg.getAccessToken(), activeOrg.getMyDomain(), null);
    }

    private Path resolveUploadRoot(String directory) {
        if (directory == null || directory.isBlank()) {
            return DEFAULT_UPLOAD_ROOT;
        }
        return Paths.get(directory).normalize();
    }
}
