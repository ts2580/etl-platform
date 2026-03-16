package com.sfdcupload.file.service;

import com.etlplatform.common.error.AppException;
import com.sfdcupload.common.SalesforceFileUpload;
import com.sfdcupload.file.dto.ExcelFile;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class FileMigrationService {
    private final FileService fileService;
    private final SalesforceFileUpload salesforceFileUpload;

    private static final long MAX_SIZE_BYTES = 35_000_000L;
    private static final long MAX_TOTAL_BATCH_SIZE = 6_000_000L;

    public void migrate(String dataId, int cycle, String accessToken, String myDomain, SseEmitter emitter) {
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
                BatchGroups groups = splitBySize(listExcelFile);

                uploadBigFiles(emitter, accessToken, myDomain, listSuccessAll, groups.big());
                uploadMediumFiles(emitter, accessToken, myDomain, listSuccessAll, groups.medium());
                uploadSmallBatches(emitter, accessToken, myDomain, listSuccessAll, groups.smallBatches());

                if (!listSuccessAll.isEmpty()) {
                    int updateCnt = fileService.updateAccFile(listSuccessAll);
                    totalProcessed += updateCnt;
                    emitter.send(SseEmitter.event().data("✔ " + updateCnt + "건 DB 반영 완료"));
                    double percent = targetCnt == 0 ? 100.0 : (totalProcessed * 100.0) / targetCnt;
                    emitter.send(SseEmitter.event().data("progress:" + percent + "," + totalProcessed + "," + targetCnt));
                }

                loopCount++;
                if (loopCount >= cycle) {
                    break;
                }
            }

            emitter.send(SseEmitter.event().data("🎉 전체 완료 : 총 " + totalProcessed + "건 처리"));
            log.info("Completed file upload migration. dataId={}, totalProcessed={}", dataId, totalProcessed);
        } catch (AppException e) {
            throw e;
        } catch (IOException e) {
            throw new AppException("SSE 이벤트 처리 중 오류가 발생했습니다.", e);
        } catch (Exception e) {
            throw new AppException("파일 마이그레이션 중 오류가 발생했습니다.", e);
        }
    }

    private BatchGroups splitBySize(List<ExcelFile> listExcelFile) {
        List<ExcelFile> big = new ArrayList<>();
        List<ExcelFile> medium = new ArrayList<>();
        List<List<ExcelFile>> smallBatches = new ArrayList<>();

        List<ExcelFile> currentSmallBatch = new ArrayList<>();
        long currentBatchSize = 0;

        for (ExcelFile excelFile : listExcelFile) {
            long fileSize = excelFile.getAppendFile() == null ? 0 : excelFile.getAppendFile().length;

            if (fileSize > MAX_SIZE_BYTES) {
                big.add(excelFile);
                continue;
            }

            if (fileSize > MAX_TOTAL_BATCH_SIZE) {
                medium.add(excelFile);
                continue;
            }

            if (currentBatchSize + fileSize < MAX_TOTAL_BATCH_SIZE && currentSmallBatch.size() < 25) {
                currentSmallBatch.add(excelFile);
                currentBatchSize += fileSize;
            } else {
                flushCurrentSmallBatch(currentSmallBatch, smallBatches, medium);
                currentSmallBatch.clear();
                currentSmallBatch.add(excelFile);
                currentBatchSize = fileSize;
            }
        }

        flushCurrentSmallBatch(currentSmallBatch, smallBatches, medium);
        return new BatchGroups(big, medium, smallBatches);
    }

    private void flushCurrentSmallBatch(List<ExcelFile> currentSmallBatch,
                                        List<List<ExcelFile>> smallBatches,
                                        List<ExcelFile> medium) {
        if (currentSmallBatch.isEmpty()) {
            return;
        }

        if (currentSmallBatch.size() == 1) {
            medium.add(currentSmallBatch.get(0));
            return;
        }

        smallBatches.add(new ArrayList<>(currentSmallBatch));
    }

    private void uploadBigFiles(SseEmitter emitter, String accessToken, String myDomain, List<ExcelFile> listSuccessAll, List<ExcelFile> listBig) throws Exception {
        if (listBig.isEmpty()) {
            return;
        }

        for (ExcelFile excelFile : listBig) {
            boolean result = salesforceFileUpload.uploadFileViaConnectAPI(
                    excelFile.getAppendFile(),
                    excelFile.getBbsAttachFileName(),
                    excelFile.getSfid(),
                    accessToken,
                    myDomain
            );
            if (result) {
                excelFile.setIsMig(1);
                listSuccessAll.add(excelFile);
                sendEvent(emitter, "✅ Connect Upload 성공, sfid :: " + excelFile.getSfid());
            } else {
                sendEvent(emitter, "‼️Connect Upload 실패!!");
            }
        }
    }

    private void uploadMediumFiles(SseEmitter emitter, String accessToken, String myDomain, List<ExcelFile> listSuccessAll, List<ExcelFile> listMedium) throws Exception {
        for (ExcelFile excelFile : listMedium) {
            boolean result = salesforceFileUpload.uploadFileViaContentVersionAPI(
                    excelFile.getAppendFile(),
                    excelFile.getBbsAttachFileName(),
                    excelFile.getSfid(),
                    accessToken,
                    myDomain
            );
            if (result) {
                excelFile.setIsMig(1);
                listSuccessAll.add(excelFile);
                sendEvent(emitter, "✅ ContentVersionAPI 단건 성공, sfid :: " + excelFile.getSfid());
            } else {
                sendEvent(emitter, "❌ ContentVersionAPI 실패!!");
            }
        }
    }

    private void uploadSmallBatches(SseEmitter emitter, String accessToken, String myDomain, List<ExcelFile> listSuccessAll, List<List<ExcelFile>> listSmallPrime) throws IOException {
        for (List<ExcelFile> excelFiles : listSmallPrime) {
            List<ExcelFile> listSuccess = salesforceFileUpload.uploadFileBatch(excelFiles, accessToken, myDomain);
            if (!listSuccess.isEmpty()) {
                listSuccessAll.addAll(listSuccess);
                sendEvent(emitter, "✅ ContentVersionAPI Batch " + listSuccess.size() + "건 성공 ");
            } else {
                sendEvent(emitter, "❌ Batch 실패");
            }
        }
    }

    private void sendEvent(SseEmitter emitter, String data) {
        try {
            emitter.send(SseEmitter.event().data(data));
        } catch (IOException e) {
            throw new AppException("SSE 전송 중 오류가 발생했습니다.", e);
        }
    }

    private record BatchGroups(List<ExcelFile> big, List<ExcelFile> medium, List<List<ExcelFile>> smallBatches) {
    }
}
