package com.etlplatform.common.health;

import java.time.Instant;

public record HealthStatusResponse(
        String module,
        String status,
        boolean dbEnabled,
        boolean salesforceConfigured,
        Instant timestamp
) {
}
