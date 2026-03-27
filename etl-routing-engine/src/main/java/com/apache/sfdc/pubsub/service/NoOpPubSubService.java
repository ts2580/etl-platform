package com.apache.sfdc.pubsub.service;

import com.etlplatform.common.error.FeatureDisabledException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpPubSubService implements PubSubService {
    @Override
    public Map<String, Object> createCdcChannel(Map<String, String> mapProperty, String token) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 CDC 채널 생성 기능을 사용할 수 없습니다.");
    }

    @Override
    public Map<String, Object> setTable(Map<String, String> mapProperty, String token) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 PubSub 적재 기능을 사용할 수 없습니다.");
    }

    @Override
    public void subscribeCDC(Map<String, String> mapProperty, Map<String, Object> mapType) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 PubSub 구독 기능을 사용할 수 없습니다.");
    }

    @Override
    public void markSlotActive(String selectedObject, String ingestionProtocol, String orgKey, Long routingRegistryId) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 슬롯 정보를 기록할 수 없습니다.");
    }

    @Override
    public void deactivateSlot(String orgKey, String selectedObject, String ingestionProtocol) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 슬롯 정보를 변경할 수 없습니다.");
    }

    @Override
    public Map<String, Object> getSlotSummary(String ingestionProtocol) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 슬롯 정보를 조회할 수 없습니다.");
    }

    @Override
    public void dropTable(Map<String, String> mapProperty) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 테이블 제거 기능을 사용할 수 없습니다.");
    }

    @Override
    public void stopRoute(String orgKey, String selectedObject, String reason) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 CDC route 중지 기능을 사용할 수 없습니다.");
    }

    @Override
    public Map<String, Object> refreshCredentials(Map<String, String> mapProperty) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 CDC credential refresh 기능을 사용할 수 없습니다.");
    }

    @Override
    public Map<String, Object> restartRoutesForOrg(String orgKey, String reason) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 CDC route restart 기능을 사용할 수 없습니다.");
    }

    @Override
    public boolean isRouteActive(String orgKey, String selectedObject) {
        return false;
    }
}
