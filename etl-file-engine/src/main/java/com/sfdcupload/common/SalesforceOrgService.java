package com.sfdcupload.common;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class SalesforceOrgService {

    private final SalesforceOrgCredentialRepository repository;

    public SalesforceOrgCredential getOrg(String orgKey) {
        return orgKey == null || orgKey.isBlank() ? null : repository.findByOrgKey(orgKey);
    }

    public SalesforceOrgCredential getDefaultOrg() {
        List<SalesforceOrgCredential> orgs = repository.findAllActive();
        return orgs.isEmpty() ? null : orgs.get(0);
    }

    public SalesforceOrgCredential resolveSelectedOrg(String orgKey) {
        SalesforceOrgCredential selected = getOrg(orgKey);
        return selected != null ? selected : getDefaultOrg();
    }

    public void storeAccessToken(String orgKey, String accessToken) {
        if (orgKey == null || orgKey.isBlank() || accessToken == null || accessToken.isBlank()) {
            return;
        }
        repository.updateAccessToken(orgKey, accessToken);
    }
}
