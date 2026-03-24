package com.sfdcupload.storage.service;

import com.etlplatform.common.error.AppException;
import com.etlplatform.common.storage.database.DatabaseConnectionTestResult;
import com.etlplatform.common.storage.database.DatabaseConnectionTestSupport;
import com.sfdcupload.storage.dto.DatabaseConnectionTestResponse;
import com.sfdcupload.storage.dto.DatabaseStorageRegistrationRequest;
import com.sfdcupload.storage.model.repository.ExternalStorageConnectionHistoryRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class DatabaseConnectionTestService {

    private final DatabaseConnectionTestSupport connectionTestSupport = new DatabaseConnectionTestSupport();
    private final ExternalStorageConnectionHistoryRepository historyRepository;

    public DatabaseConnectionTestResponse test(DatabaseStorageRegistrationRequest request) {
        validate(request);
        try {
            DatabaseConnectionTestResult result = connectionTestSupport.test(request);
            historyRepository.insert(null, "TEST", true, "등록 전 연결 테스트 성공", request.getVendor() + " / " + request.getJdbcUrl());
            return DatabaseConnectionTestResponse.builder()
                    .success(result.success())
                    .message(result.message())
                    .build();
        } catch (AppException e) {
            historyRepository.insert(null, "TEST", false, "등록 전 연결 테스트 실패", e.getMessage());
            throw e;
        }
    }

    public void validate(DatabaseStorageRegistrationRequest request) {
        com.etlplatform.common.storage.database.DatabaseStorageRegistrationValidator.validate(request);
    }
}
