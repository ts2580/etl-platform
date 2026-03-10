package com.apache.sfdc.streaming.controller;

import com.apache.sfdc.common.SalesforceOAuth;
import com.apache.sfdc.streaming.service.StreamingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequiredArgsConstructor
public class StreamingController {
    private final StreamingService routerService;

    // AWS ALB가 Health Check를 하기 위한 경로
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("OK");
    }

    @PostMapping("/streaming")
    public String setPushTopic(@RequestBody String strJson) throws Exception {

        // Streaming API로는 PushTopic events, generic events, platform events, Change Data Capture events 다 받음
        // PushTopic event는 streaming으로 받자.
        // Bayeux 프로토콜과 CometD 클라이언트 기반.

        ObjectMapper objectMapper = new ObjectMapper();

        // strJson을 Map으로 변환 , mapProperty에는 아파치 카멜로 salesforce component 접속정보
        Map<String, String> mapProperty = objectMapper.readValue(strJson, Map.class);

        // 토큰 생성
        String token = mapProperty.get("accessToken");

        // 테이블 생성 후 데이터 넣기
        Map<String, Object> mapReturn = routerService.setTable(mapProperty, token);

        // 푸시토픽 넣어주기
        String pushTopic = routerService.setPushTopic(mapProperty, mapReturn, token);

        // 푸시토픽 구독 및 DB 삽입
        routerService.subscribePushTopic(mapProperty, token, (Map<String, Object>) mapReturn.get("mapType"));

        return "모든 시퀸스 성공";
    }



}