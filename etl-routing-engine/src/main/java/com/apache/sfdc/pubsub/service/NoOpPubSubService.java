package com.apache.sfdc.pubsub.service;

import com.etlplatform.common.error.FeatureDisabledException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpPubSubService implements PubSubService {
    @Override
    public Map<String, Object> setTable(Map<String, String> mapProperty, String token) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 PubSub 적재 기능을 사용할 수 없습니다.");
    }

    @Override
    public void subscribeCDC(Map<String, String> mapProperty, Map<String, Object> mapType) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 PubSub 구독 기능을 사용할 수 없습니다.");
    }
}
