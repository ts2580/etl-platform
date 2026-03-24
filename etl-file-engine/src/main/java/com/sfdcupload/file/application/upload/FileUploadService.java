package com.sfdcupload.file.application.upload;

import com.etlplatform.common.error.AppException;
import com.sfdcupload.file.application.history.FileUploadHistoryWriter;
import com.sfdcupload.file.domain.FileUploadResult;
import com.sfdcupload.file.domain.FileUploadStrategy;
import com.sfdcupload.file.domain.MigrationFile;
import com.sfdcupload.file.domain.result.FileUploadItemResult;
import com.sfdcupload.file.domain.result.FileUploadSummary;
import com.sfdcupload.file.source.FileSource;
import com.sfdcupload.file.target.FileUploadTarget;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class FileUploadService {
    private static final long MAX_BATCH_BYTES = 6_000_000L;
    private static final int MAX_BATCH_COUNT = 25;

    private final FileSource fileSource;
    private final FileUploadTarget fileUploadTarget;
    private final FileUploadHistoryWriter fileUploadHistoryWriter;

    public FileUploadSummary uploadDirectory(Path rootPath, String accessToken, String myDomain, SseEmitter emitter) {
        try {
            List<MigrationFile> files = fileSource.loadFiles(rootPath);
            if (emitter != null) {
                emitter.send(SseEmitter.event().data("progress: 0,0," + files.size()));
            }

            if (files.isEmpty()) {
                if (emitter != null) {
                    emitter.send(SseEmitter.event().data("✅ 업로드할 파일이 없습니다"));
                }
                return writeSummary(rootPath, List.of());
            }

            List<List<MigrationFile>> batches = splitIntoBatches(files);
            List<FileUploadItemResult> itemResults = new ArrayList<>();
            int processed = 0;

            for (List<MigrationFile> batch : batches) {
                if (batch.size() == 1) {
                    MigrationFile file = batch.get(0);
                    FileUploadResult result = fileUploadTarget.upload(file, FileUploadStrategy.CONTENT_VERSION_SINGLE, accessToken, myDomain);
                    itemResults.add(toItemResult(file, result));
                    processed += handleSingleResult(emitter, file, result);
                } else {
                    List<FileUploadResult> results = fileUploadTarget.uploadBatch(batch, accessToken, myDomain);
                    processed += handleBatchResults(emitter, batch, results, itemResults);
                }

                if (emitter != null) {
                    double percent = files.isEmpty() ? 100.0 : (processed * 100.0) / files.size();
                    emitter.send(SseEmitter.event().data("progress:" + percent + "," + processed + "," + files.size()));
                }
            }

            if (emitter != null) {
                emitter.send(SseEmitter.event().data("🎉 전체 완료 : 총 " + processed + "건 업로드"));
            }
            return writeSummary(rootPath, itemResults);
        } catch (AppException e) {
            throw e;
        } catch (IOException e) {
            throw new AppException("파일 업로드 중 IO 오류가 발생했습니다.", e);
        } catch (Exception e) {
            throw new AppException("파일 업로드 중 오류가 발생했습니다.", e);
        }
    }

    private int handleSingleResult(SseEmitter emitter, MigrationFile file, FileUploadResult result) throws IOException {
        if (result.isSuccess()) {
            if (emitter != null) {
                emitter.send(SseEmitter.event().data(
                        "✅ 업로드 성공 :: " + file.getFileName()
                                + " (ContentVersionId=" + result.getContentVersionId()
                                + ", ContentDocumentId=" + result.getContentDocumentId() + ")"
                ));
            }
            return 1;
        }

        if (emitter != null) {
            emitter.send(SseEmitter.event().data("❌ 업로드 실패 :: " + file.getFileName()));
        }
        return 0;
    }

    private int handleBatchResults(SseEmitter emitter,
                                   List<MigrationFile> files,
                                   List<FileUploadResult> results,
                                   List<FileUploadItemResult> itemResults) throws IOException {
        int successCount = 0;
        for (int i = 0; i < files.size(); i++) {
            MigrationFile file = files.get(i);
            FileUploadResult result = i < results.size() ? results.get(i) : null;
            itemResults.add(toItemResult(file, result));
            if (result != null && result.isSuccess()) {
                successCount++;
                if (emitter != null) {
                    emitter.send(SseEmitter.event().data(
                            "✅ 업로드 성공 :: " + file.getFileName()
                                    + " (ContentVersionId=" + result.getContentVersionId()
                                    + ", ContentDocumentId=" + result.getContentDocumentId() + ")"
                    ));
                }
            } else if (emitter != null) {
                emitter.send(SseEmitter.event().data("❌ 업로드 실패 :: " + file.getFileName()));
            }
        }
        return successCount;
    }

    private FileUploadItemResult toItemResult(MigrationFile file, FileUploadResult result) {
        return FileUploadItemResult.builder()
                .sourceId(file.getSourceId())
                .fileName(file.getFileName())
                .success(result != null && result.isSuccess())
                .contentVersionId(result != null ? result.getContentVersionId() : null)
                .contentDocumentId(result != null ? result.getContentDocumentId() : null)
                .message(result != null ? result.getMessage() : "NO_RESULT")
                .build();
    }

    private FileUploadSummary writeSummary(Path rootPath, List<FileUploadItemResult> itemResults) throws IOException {
        int successCount = (int) itemResults.stream().filter(FileUploadItemResult::isSuccess).count();
        FileUploadSummary summary = FileUploadSummary.builder()
                .uploadRoot(rootPath.toString())
                .totalCount(itemResults.size())
                .successCount(successCount)
                .failureCount(itemResults.size() - successCount)
                .items(itemResults)
                .historyFile(null)
                .build();

        Path historyPath = fileUploadHistoryWriter.write(summary);
        return FileUploadSummary.builder()
                .uploadRoot(summary.getUploadRoot())
                .totalCount(summary.getTotalCount())
                .successCount(summary.getSuccessCount())
                .failureCount(summary.getFailureCount())
                .items(summary.getItems())
                .historyFile(historyPath.toString())
                .build();
    }

    private List<List<MigrationFile>> splitIntoBatches(List<MigrationFile> files) {
        List<List<MigrationFile>> batches = new ArrayList<>();
        List<MigrationFile> currentBatch = new ArrayList<>();
        long currentBatchBytes = 0L;

        for (MigrationFile file : files) {
            boolean shouldFlush = !currentBatch.isEmpty()
                    && (currentBatch.size() >= MAX_BATCH_COUNT || currentBatchBytes + file.getSize() > MAX_BATCH_BYTES);

            if (shouldFlush) {
                batches.add(new ArrayList<>(currentBatch));
                currentBatch.clear();
                currentBatchBytes = 0L;
            }

            currentBatch.add(file);
            currentBatchBytes += file.getSize();
        }

        if (!currentBatch.isEmpty()) {
            batches.add(currentBatch);
        }

        return batches;
    }
}
