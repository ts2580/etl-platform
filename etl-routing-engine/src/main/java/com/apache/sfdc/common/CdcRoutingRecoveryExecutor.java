package com.apache.sfdc.common;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.apache.sfdc.pubsub.service.PubSubService;

import java.util.Map;

@Component
@RequiredArgsConstructor
public class CdcRoutingRecoveryExecutor implements RoutingRecoveryExecutor {

    private static final Logger log = LoggerFactory.getLogger(CdcRoutingRecoveryExecutor.class);

    private final PubSubService pubSubService;

    @Override
    public String protocol() {
        return "CDC";
    }

    @Override
    public int recover(Map<String, String> mapProperty, String actor) throws Exception {
        String selectedObject = mapProperty.get("selectedObject");
        String orgKey = mapProperty.get("orgKey");
        String accessToken = mapProperty.get("accessToken");

        mapProperty.put("skipInitialLoad", "true");
        pubSubService.createCdcChannel(mapProperty, accessToken);
        Map<String, Object> mapReturn = pubSubService.setTable(mapProperty, accessToken);
        pubSubService.subscribeCDC(mapProperty, castMap(mapReturn.get("mapType")));
        pubSubService.markSlotActive(selectedObject, "CDC", orgKey, null);

        log.info("[routing-recovery:{}] cdc route revived without registry rewrite. orgKey={}, selectedObject={}",
                actor,
                orgKey,
                selectedObject);

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
