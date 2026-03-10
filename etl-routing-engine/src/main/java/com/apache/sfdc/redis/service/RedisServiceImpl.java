package com.apache.sfdc.redis.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * RedisService의 구현체
 */
@Service
@RequiredArgsConstructor
public class RedisServiceImpl implements RedisService {

    private final RedisTemplate<String, Object> redisTemplate;

    /**
     * Redis 값을 등록/수정
     *
     * @param key {String} - redis key
     * @param value {String} - redis value
     */
    @Override
    public void setValues(String key, String value) {
        ValueOperations<String, Object> values = redisTemplate.opsForValue();
        values.set(key, value);
    }

    /**
     * Redis 값을 등록/수정
     *
     * @param key {String} - redis key
     * @param value {String} - redis value
     * @param duration {Duration} - redis 값 메모리 상의 유효시간(ms)
     */
    @Override
    public void setValues(String key, String value, Duration duration) {
        ValueOperations<String, Object> values = redisTemplate.opsForValue();
        values.set(key, value, duration);
    }

    /**
     * Redis 키를 기반으로 값을 조회
     *
     * @param key {String} - redis key
     * @return {String} - redis value 값 반환 or 미 존재시 빈 값 반환
     */
    @Override
    public String getValue(String key) {
        ValueOperations<String, Object> values = redisTemplate.opsForValue();
        if (values.get(key) == null) return "검색된 값이 없습니다.";
        return String.valueOf(values.get(key));
    }

    /**
     * Redis 키값을 기반으로 row 삭제합니다.
     * @param key {String} - 삭제할 Key
     */
    @Override
    public String deleteValue(String key) {
        redisTemplate.delete(key);
        return "값 제거 성공했습니다.";
    }
}