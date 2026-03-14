package com.sfdcupload.file.controller;

import com.etlplatform.common.error.AppException;
import com.etlplatform.common.validation.RequestValidationUtils;
import com.sfdcupload.common.SalesforceFileUpload;
import com.sfdcupload.common.SalesforceTokenManager;
import com.sfdcupload.file.dto.ExcelFile;
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
import java.util.ArrayList;
import java.util.List;

@RestController
@RequiredArgsConstructor
@Slf4j
public class FileController {

    private final FileService fileService;
    private final SalesforceFileUpload salesforceFileUpload;
    private final SalesforceTokenManager tokenManager;

    final long MAX_SIZE_BYTES = 35_000_000L;
    final long MAX_TOTAL_BATCH_SIZE = 6_000_000L;
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
        String sanitizedDataId = RequestValidationUtils.requireSimpleKey(dataId, "dataId");
        int validatedCycle = RequestValidationUtils.requirePositive(cycle, "cycle");
        SseEmitter emitter = new SseEmitter(0L);
        String accessToken = tokenManager.getAccessToken(session);

        if (accessToken == null) {
            throw new AppException("로그인이 필요합니다. 먼저 OAuth 로그인을 진행해 주세요.");
        }

        log.info("Starting file upload migration. dataId={}, cycle={}", sanitizedDataId, validatedCycle);
        new Thread(() -> {
            try {
                int totalProcessed = 0;
                int loopCount = 0;
                int targetCnt = fileService.totalAccFile();
                emitter.send(SseEmitter.event().data("progress: 0," + totalProcessed + "," + targetCnt));

                while (true) {
                    List<ExcelFile> listExcelFile = fileService.findAccFile();
                    if (listExcelFile.isEmpty()) {
                        emitter.send(SseEmitter.event().data("✅ 더 이상 처리할 항목이 없습니다"));
                        break;
                    }

                    List<ExcelFile> listSuccessAll = new ArrayList<>();
                    List<ExcelFile> listBig = new ArrayList<>();
                    List<ExcelFile> listMedium = new ArrayList<>();
                    List<List<ExcelFile>> listSmallPrime = new ArrayList<>();
                    List<ExcelFile> listSmall = new ArrayList<>();
                    long currentBatchSize = 0;

                    for (ExcelFile excelFile : listExcelFile) {
                        long fileSize = excelFile.getAppendFile().length;
                        if (fileSize > MAX_SIZE_BYTES) {
                            listBig.add(excelFile);
                        } else if (fileSize > MAX_TOTAL_BATCH_SIZE) {
                            listMedium.add(excelFile);
                        } else {
                            if (currentBatchSize + fileSize < MAX_TOTAL_BATCH_SIZE && listSmall.size() < 25) {
                                listSmall.add(excelFile);
                                currentBatchSize += fileSize;
                            } else {
                                if (listSmall.size() == 1) {
                                    listMedium.add(listSmall.get(0));
                                } else if (!listSmall.isEmpty()) {
                                    listSmallPrime.add(new ArrayList<>(listSmall));
                                }
                                listSmall.clear();
                                listSmall.add(excelFile);
                                currentBatchSize = fileSize;
                            }
                        }
                    }

                    if (!listSmall.isEmpty()) {
                        if (listSmall.size() == 1) {
                            listMedium.add(listSmall.get(0));
                        } else {
                            listSmallPrime.add(new ArrayList<>(listSmall));
                        }
                    }

                    uploadBigFiles(emitter, accessToken, listSuccessAll, listBig);
                    uploadMediumFiles(emitter, accessToken, listSuccessAll, listMedium);
                    uploadSmallBatches(emitter, accessToken, listSuccessAll, listSmallPrime);

                    if (!listSuccessAll.isEmpty()) {
                        int updateCnt = fileService.updateAccFile(listSuccessAll);
                        totalProcessed += updateCnt;
                        emitter.send(SseEmitter.event().data("✔ " + updateCnt + "건 DB 반영 완료"));
                        double percent = targetCnt == 0 ? 100.0 : (totalProcessed * 100.0) / targetCnt;
                        emitter.send(SseEmitter.event().data("progress:" + percent + "," + totalProcessed + "," + targetCnt));
                    }

                    loopCount++;
                    if (loopCount >= validatedCycle) {
                        break;
                    }
                }

                emitter.send(SseEmitter.event().data("🎉 전체 완료 : 총 " + totalProcessed + "건 처리"));
                log.info("Completed file upload migration. dataId={}, totalProcessed={}", sanitizedDataId, totalProcessed);
            } catch (Exception e) {
                log.error("업로드 진행 중 예외 발생. dataId={}", sanitizedDataId, e);
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

    private void uploadBigFiles(SseEmitter emitter, String accessToken, List<ExcelFile> listSuccessAll, List<ExcelFile> listBig) {
        if (listBig.isEmpty()) {
            return;
        }
        try {
            for (ExcelFile excelFile : listBig) {
                boolean result = salesforceFileUpload.uploadFileViaConnectAPI(excelFile.getAppendFile(), excelFile.getBbsAttachFileName(), excelFile.getSfid(), accessToken);
                if (result) {
                    excelFile.setIsMig(1);
                    listSuccessAll.add(excelFile);
                    emitter.send(SseEmitter.event().data("✅ Connect Upload 성공, sfid :: " + excelFile.getSfid()));
                } else {
                    emitter.send(SseEmitter.event().data("‼️Connect Upload 실패!!"));
                }
            }
        } catch (Exception e) {
            log.error("Connect upload batch 처리 중 예외", e);
            sendError(emitter, e);
        }
    }

    private void uploadMediumFiles(SseEmitter emitter, String accessToken, List<ExcelFile> listSuccessAll, List<ExcelFile> listMedium) {
        if (listMedium.isEmpty()) {
            return;
        }
        try {
            for (ExcelFile excelFile : listMedium) {
                boolean result = salesforceFileUpload.uploadFileViaContentVersionAPI(excelFile.getAppendFile(), excelFile.getBbsAttachFileName(), excelFile.getSfid(), accessToken);
                if (result) {
                    excelFile.setIsMig(1);
                    listSuccessAll.add(excelFile);
                    emitter.send(SseEmitter.event().data("✅ ContentVersionAPI 단건 성공, sfid :: " + excelFile.getSfid()));
                } else {
                    emitter.send(SseEmitter.event().data("❌ ContentVersionAPI 실패!!"));
                }
            }
        } catch (Exception e) {
            log.error("ContentVersion single upload 중 예외", e);
            sendError(emitter, e);
        }
    }

    private void uploadSmallBatches(SseEmitter emitter, String accessToken, List<ExcelFile> listSuccessAll, List<List<ExcelFile>> listSmallPrime) {
        if (listSmallPrime.isEmpty()) {
            return;
        }
        for (List<ExcelFile> excelFiles : listSmallPrime) {
            try {
                List<ExcelFile> listSuccess = salesforceFileUpload.uploadFileBatch(excelFiles, accessToken);
                if (!listSuccess.isEmpty()) {
                    listSuccessAll.addAll(listSuccess);
                    emitter.send(SseEmitter.event().data("✅ ContentVersionAPI Batch " + listSuccess.size() + "건 성공 "));
                } else {
                    emitter.send(SseEmitter.event().data("❌ Batch 실패"));
                }
            } catch (Exception e) {
                log.error("Batch 업로드 중 예외", e);
                sendError(emitter, e);
            }
        }
    }

    private void sendError(SseEmitter emitter, Exception e) {
        try {
            emitter.send(SseEmitter.event().data("❌ 오류 : " + e.getMessage()));
        } catch (IOException ioException) {
            throw new RuntimeException(ioException);
        }
    }
}
