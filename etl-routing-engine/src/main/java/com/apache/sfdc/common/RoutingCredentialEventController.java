package com.apache.sfdc.common;

import com.apache.sfdc.pubsub.service.PubSubService;
import com.apache.sfdc.streaming.service.StreamingService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/internal/routing")
public class RoutingCredentialEventController {

    private final StreamingService streamingService;
    private final PubSubService pubSubService;

    @PostMapping("/credential-updated")
    public Map<String, Object> credentialUpdated(@RequestBody CredentialUpdatedRequest request) throws Exception {
        Map<String, Object> result = new HashMap<>();
        Map<String, Object> streamingResult = streamingService.restartRoutesForOrg(request.orgKey(), request.reason());
        Map<String, Object> cdcResult = pubSubService.restartRoutesForOrg(request.orgKey(), request.reason());

        result.put("status", "OK");
        result.put("orgKey", request.orgKey());
        result.put("credentialVersion", request.credentialVersion());
        result.put("reason", request.reason());
        result.put("receivedAt", Instant.now().toString());
        result.put("streaming", streamingResult);
        result.put("cdc", cdcResult);
        return result;
    }

    public record CredentialUpdatedRequest(
            String orgKey,
            Long credentialVersion,
            String reason
    ) {
    }
}
