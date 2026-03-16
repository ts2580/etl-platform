package com.apache.sfdc.streaming.service;

import com.etlplatform.common.error.FeatureDisabledException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpStreamingService implements StreamingService {
    @Override
    public Map<String, Object> setTable(Map<String, String> mapProperty, String token) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 Streaming 적재 기능을 사용할 수 없습니다.");
    }

    @Override
    public String setPushTopic(Map<String, String> mapProperty, Map<String, Object> mapReturn, String token) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 PushTopic 기능을 사용할 수 없습니다.");
    }

    @Override
    public void subscribePushTopic(Map<String, String> mapProperty, String token, Map<String, Object> mapType) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 Streaming 구독 기능을 사용할 수 없습니다.");
    }

    @Override
    public void dropTable(Map<String, String> mapProperty) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 테이블 제거 기능을 사용할 수 없습니다.");
    }
}
