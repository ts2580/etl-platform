package com.etl.sfdc.storage.service;

import com.etl.sfdc.storage.dto.DatabaseConnectionTestResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageRegistrationRequest;
import com.etl.sfdc.storage.model.repository.ExternalStorageConnectionHistoryRepository;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.DatabaseConnectionTestResult;
import com.etlplatform.common.storage.database.DatabaseConnectionTestSupport;
import com.etlplatform.common.storage.database.DatabaseJdbcMetadata;
import com.etlplatform.common.storage.database.DatabaseJdbcSupport;
import com.etlplatform.common.storage.database.DatabaseStorageRegistrationValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class DatabaseConnectionTestService {

    private final DatabaseConnectionTestSupport connectionTestSupport = new DatabaseConnectionTestSupport();
    private final ExternalStorageConnectionHistoryRepository historyRepository;

    public DatabaseConnectionTestResponse test(DatabaseStorageRegistrationRequest request) {
        validate(request);
        String start = String.format("[DB 연결 테스트] 시작. vendor=%s, authMethod=%s, user=%s, rawUrl=%s, port=%s, schemaName=%s, databaseName=%s",
                request.getVendor(), request.getAuthMethod(), request.getUsername(), request.getJdbcUrl(), request.getPort(), request.getSchemaName(), request.getDatabaseName());
        log.info(start);
        System.out.println(start);

        try {
            DatabaseConnectionTestResult result = connectionTestSupport.test(request);
            DatabaseJdbcMetadata metadata = DatabaseJdbcSupport.parseMetadata(
                    request.getVendor(),
                    request.getJdbcUrl(),
                    request.getPort()
            );
            String jdbcUrl = DatabaseJdbcSupport.buildJdbcUrl(request.getVendor(), metadata);
            String success = String.format("[DB 연결 테스트] 성공. vendor=%s, authMethod=%s, jdbcUrl=%s, metadataHost=%s, metadataPort=%s",
                    request.getVendor(), request.getAuthMethod(), jdbcUrl, metadata.host(), metadata.port());
            log.info(success);
            System.out.println(success);
            historyRepository.insert(null, "TEST", true, "등록 전 연결 테스트 성공", request.getVendor() + " / " + jdbcUrl);
            return DatabaseConnectionTestResponse.builder()
                    .success(result.success())
                    .message(result.message())
                    .build();
        } catch (AppException e) {
            String err = String.format("[DB 연결 테스트] 실패. vendor=%s, authMethod=%s, rawUrl=%s, port=%s, reason=%s, causeChain=%s",
                    request.getVendor(), request.getAuthMethod(), request.getJdbcUrl(), request.getPort(), e.getMessage(), summarizeThrowableChain(e));
            log.error("Database connection test failed in API service. vendor={}, authMethod={}, rawUrl={}, port={}, causeChain={}",
                    request.getVendor(), request.getAuthMethod(), request.getJdbcUrl(), request.getPort(), summarizeThrowableChain(e), e);
            System.err.println(err);
            System.err.println(stackMessage(e));
            try {
                historyRepository.insert(null, "TEST", false, "등록 전 연결 테스트 실패", e.getMessage());
            } catch (Exception historyError) {
                log.warn("Failed to record connection test failure history", historyError);
                System.err.println("[DB 연결 테스트] 실패 이력 저장 실패: " + historyError.getMessage());
            }
            throw e;
        }
    }

    private String summarizeThrowableChain(Throwable throwable) {
        StringBuilder builder = new StringBuilder();
        Throwable current = throwable;
        while (current != null) {
            if (builder.length() > 0) {
                builder.append(" -> ");
            }
            builder.append(current.getClass().getSimpleName())
                    .append('(')
                    .append(current.getMessage())
                    .append(')');
            current = current.getCause();
        }
        return builder.toString();
    }

    private String stackMessage(Throwable t) {
        java.io.StringWriter sw = new java.io.StringWriter();
        t.printStackTrace(new java.io.PrintWriter(sw));
        return sw.toString();
    }

    public void validate(DatabaseStorageRegistrationRequest request) {
        DatabaseStorageRegistrationValidator.validate(request);
    }
}
