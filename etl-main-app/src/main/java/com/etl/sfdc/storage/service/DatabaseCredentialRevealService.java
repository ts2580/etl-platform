package com.etl.sfdc.storage.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class DatabaseCredentialRevealService {

    private static final long TTL_SECONDS = 60;

    private final Map<String, RevealTicket> activeTokens = new ConcurrentHashMap<>();

    private record RevealTicket(
            String token,
            Long storageId,
            String requesterIdentity,
            Instant expiresAt,
            AtomicBoolean used
    ) {
    }

    public String issue(Long storageId, String requesterIdentity) {
        String token = UUID.randomUUID().toString().replace("-", "");
        RevealTicket ticket = new RevealTicket(
                token,
                storageId,
                normalizeIdentity(requesterIdentity),
                Instant.now().plusSeconds(TTL_SECONDS),
                new AtomicBoolean(false)
        );
        activeTokens.put(token, ticket);
        return token;
    }

    public boolean consumeIfValid(Long storageId, String token, String requesterIdentity) {
        RevealTicket ticket = activeTokens.get(token);
        if (ticket == null) {
            return false;
        }

        if (!ticket.storageId.equals(storageId)) {
            return false;
        }

        if (!normalizeIdentity(requesterIdentity).equals(ticket.requesterIdentity)) {
            return false;
        }

        if (ticket.expiresAt.isBefore(Instant.now())) {
            activeTokens.remove(token);
            return false;
        }

        if (!ticket.used.compareAndSet(false, true)) {
            return false;
        }

        activeTokens.remove(token);
        return true;
    }

    public String summarize(String token) {
        if (token == null || token.isBlank()) {
            return "-";
        }
        return token.substring(0, Math.min(6, token.length())) + "...";
    }

    @Scheduled(fixedDelay = 60_000)
    public void purgeExpired() {
        Instant now = Instant.now();
        activeTokens.entrySet().removeIf(entry -> {
            RevealTicket ticket = entry.getValue();
            return ticket.expiresAt.isBefore(now) || ticket.used.get();
        });
    }

    private String normalizeIdentity(String requesterIdentity) {
        return (requesterIdentity == null || requesterIdentity.isBlank()) ? "unknown" : requesterIdentity;
    }
}
