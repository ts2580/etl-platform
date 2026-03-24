package com.sfdcupload.common;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class SalesforceOrgService {

    private final ObjectProvider<SalesforceOrgCredentialRepository> repositoryProvider;

    public SalesforceOrgCredential getOrg(String orgKey) {
        SalesforceOrgCredentialRepository repository = repository();
        return repository == null || orgKey == null || orgKey.isBlank() ? null : repository.findByOrgKey(orgKey);
    }

    public SalesforceOrgCredential getDefaultOrg() {
        SalesforceOrgCredentialRepository repository = repository();
        if (repository == null) {
            return null;
        }

        List<SalesforceOrgCredential> orgs = repository.findAllActive();
        return orgs.isEmpty() ? null : orgs.get(0);
    }

    public SalesforceOrgCredential resolveSelectedOrg(String orgKey) {
        SalesforceOrgCredential selected = getOrg(orgKey);
        return selected != null ? selected : getDefaultOrg();
    }

    public void storeAccessToken(String orgKey, String accessToken) {
        SalesforceOrgCredentialRepository repository = repository();
        if (repository == null || orgKey == null || orgKey.isBlank() || accessToken == null || accessToken.isBlank()) {
            return;
        }
        repository.updateAccessToken(orgKey, accessToken);
    }

    private SalesforceOrgCredentialRepository repository() {
        return repositoryProvider.getIfAvailable();
    }
}
