package com.etl.sfdc.config.model.service;

import com.etlplatform.common.error.FeatureDisabledException;
import com.etl.sfdc.config.model.dto.User;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpConfigService implements ConfigService {
    @Override
    public User getUserDes(String userName) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 config 기능을 사용할 수 없습니다.");
    }
}
