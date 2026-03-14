package com.sfdcupload.file.service;

import com.etlplatform.common.error.FeatureDisabledException;
import com.sfdcupload.file.dto.ExcelFile;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpFileService implements FileService {
    @Override
    public int totalAccFile() {
        throw new FeatureDisabledException("DB 비활성 모드에서는 파일 마이그레이션 상태 조회를 사용할 수 없습니다.");
    }

    @Override
    public List<ExcelFile> findAccFile() {
        throw new FeatureDisabledException("DB 비활성 모드에서는 파일 조회를 사용할 수 없습니다.");
    }

    @Override
    public int updateAccFile(List<ExcelFile> listFile) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 파일 상태 업데이트를 사용할 수 없습니다.");
    }
}
