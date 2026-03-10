package com.apache.sfdc.redis.controller;

import com.apache.sfdc.redis.dto.RedisDTO;
import com.apache.sfdc.redis.service.RedisService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/redis")
public class RedisController {

    private final RedisService redisService;

    /**
     * Redis의 값을 조회
     *
     * @param key {String} - 조회할 키
     * @return result {String} 조회된 값
     */
    @GetMapping("/getValue")
    public String getValue(@RequestParam(value="redisKey") String key) {

        String result = redisService.getValue(key);

        System.out.println(key);
        System.out.println(result);

        return result;
    }

    /**
     * Redis의 값을 추가/수정
     *
     * @param redisDTO {RedisDTO} - 세팅할 key, value
     * @return result {String} 성공여부
     */
    @PostMapping("/setValue")
    public String setValue(@RequestBody RedisDTO redisDTO) {

        redisService.setValues(redisDTO.getKey(), redisDTO.getValue());

        return "세팅 성공했습니다.";
    }

    /**
     * Redis의 key 값을 기반으로 row를 제거
     *
     * @param key {String} - 제거할 키
     */
    @PostMapping("/deleteValue")
    public String deleteRow(@RequestBody String key) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();

        RedisDTO jn = mapper.readValue(key, RedisDTO.class);

        String returnParam = redisService.deleteValue(jn.getKey());
        return "값 제거 성공했습니다.";
    }
}