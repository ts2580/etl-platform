package com.etl.sfdc.storage.service;

import com.etl.sfdc.storage.dto.DatabaseConnectionTestResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageRegistrationRequest;
import com.etl.sfdc.storage.model.repository.ExternalStorageConnectionHistoryRepository;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.DatabaseConnectionLogFormatter;
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
        String start = "[DB 연결 테스트] 시작. " + DatabaseConnectionLogFormatter.requestSummary(request);
        log.info(start);
        System.out.println(start);

        try {
            DatabaseConnectionTestResult result = connectionTestSupport.test(request);
            DatabaseJdbcMetadata metadata = DatabaseJdbcSupport.parseMetadata(
                    request.getVendor(),
                    request.getJdbcUrl(),
                    request.getPort()
            );
            if (request.getVendor() == com.etlplatform.common.storage.database.DatabaseVendor.POSTGRESQL
                    && metadata.databaseName() == null
                    && request.getDatabaseName() != null
                    && !request.getDatabaseName().isBlank()) {
                metadata = new DatabaseJdbcMetadata(metadata.host(), metadata.port(), request.getDatabaseName().trim(), metadata.serviceName(), metadata.sid());
            }
            String jdbcUrl = DatabaseJdbcSupport.buildJdbcUrl(request.getVendor(), metadata);
            String success = "[DB 연결 테스트] 성공. "
                    + DatabaseConnectionLogFormatter.outcomeSummary(request.getVendor(), request.getAuthMethod(), jdbcUrl, metadata);
            log.info(success);
            System.out.println(success);
            historyRepository.insert(null, "TEST", true, "등록 전 연결 테스트 성공", request.getVendor() + " / " + jdbcUrl);
            return DatabaseConnectionTestResponse.builder()
                    .success(result.success())
                    .message(result.message())
                    .build();
        } catch (AppException e) {
            String err = "[DB 연결 테스트] 실패. "
                    + DatabaseConnectionLogFormatter.requestSummary(request)
                    + ", reason=" + e.getMessage()
                    + ", causeChain=" + summarizeThrowableChain(e);
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
