package com.apache.sfdc.redis.service;

import com.etlplatform.common.error.FeatureDisabledException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpRedisService implements RedisService {
    @Override
    public void setValues(String key, String value) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 Redis 기능을 사용할 수 없습니다.");
    }

    @Override
    public void setValues(String key, String value, Duration duration) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 Redis 기능을 사용할 수 없습니다.");
    }

    @Override
    public String getValue(String key) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 Redis 기능을 사용할 수 없습니다.");
    }

    @Override
    public String deleteValue(String key) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 Redis 기능을 사용할 수 없습니다.");
    }
}
