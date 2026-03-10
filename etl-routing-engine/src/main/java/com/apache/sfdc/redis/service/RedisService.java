package com.apache.sfdc.redis.service;

import org.springframework.stereotype.Service;

import java.time.Duration;


@Service
public interface RedisService {

    // 값 등록 / 수정
    void setValues(String key, String value);

    // 위와 같음. 여기에 duration(메모리에 데이터가 유지될 시간을 지정) 추가
    void setValues(String key, String value, Duration duration);

    // 값 조회
    String getValue(String key);

    // 값 삭제
    String deleteValue(String key);
}