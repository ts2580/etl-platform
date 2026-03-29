package com.apache.sfdc.common;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.apache.sfdc.streaming.service.StreamingService;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class StreamingRoutingRecoveryExecutor implements RoutingRecoveryExecutor {

    private static final Logger log = LoggerFactory.getLogger(StreamingRoutingRecoveryExecutor.class);

    private final StreamingService streamingService;

    @Override
    public String protocol() {
        return "STREAMING";
    }

    @Override
    public int recover(Map<String, String> mapProperty, String actor) throws Exception {
        String accessToken = mapProperty.get("accessToken");
        mapProperty.put("skipInitialLoad", "true");

        Map<String, Object> mapReturn = streamingService.setTable(mapProperty, accessToken);
        streamingService.setPushTopic(mapProperty, mapReturn, accessToken);
        streamingService.subscribePushTopic(mapProperty, accessToken, castMap(mapReturn.get("mapType")));

        log.info("[routing-recovery:{}] streaming route revived without registry rewrite. orgKey={}, selectedObject={}",
                actor,
                mapProperty.get("orgKey"),
                mapProperty.get("selectedObject"));

        return asInt(mapReturn.get("initialLoadCount"));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> castMap(Object value) {
        if (value instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        return Map.of();
    }

    private int asInt(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value == null) {
            return 0;
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
