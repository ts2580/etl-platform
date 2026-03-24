package com.etl.sfdc.storage.service;

import com.etl.sfdc.storage.dto.DatabaseStoredCredentialDecryptRequest;
import com.etl.sfdc.storage.model.repository.ExternalDatabaseStorageRepository;
import com.etl.sfdc.storage.model.repository.ExternalStorageConnectionHistoryRepository;
import com.etl.sfdc.storage.support.CertificateCredentialMetaSerializer;
import com.etl.sfdc.storage.support.DatabaseCredentialEncryptor;
import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etlplatform.common.storage.database.DatabaseCertificateAuthRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class DatabaseStorageQueryServiceTest {

    private static final String KEY_PROPERTY = "APP_DB_CREDENTIAL_KEY";

    @Mock
    private ExternalDatabaseStorageRepository externalDatabaseStorageRepository;

    @Mock
    private ExternalStorageConnectionHistoryRepository historyRepository;

    @Mock
    private DatabaseCredentialRevealService credentialRevealService;

    private DatabaseStorageQueryService queryService;
    private DatabaseCredentialEncryptor credentialEncryptor;
    private DatabaseCertificateMetaService certificateMetaService;

    @BeforeEach
    void setUp() {
        System.setProperty(KEY_PROPERTY, "test-secret-key");
        credentialEncryptor = new DatabaseCredentialEncryptor();
        certificateMetaService = new DatabaseCertificateMetaService(new CertificateCredentialMetaSerializer());
        queryService = new DatabaseStorageQueryService(
                externalDatabaseStorageRepository,
                historyRepository,
                credentialEncryptor,
                certificateMetaService,
                credentialRevealService
        );
    }

    @AfterEach
    void tearDown() {
        System.clearProperty(KEY_PROPERTY);
    }

    @Test
    void decryptStoredCredentialsUsesAppCredentialKeyFallbackForPasswordStorage() {
        String encryptedPassword = credentialEncryptor.encrypt("pw-secret", null);
        given(externalDatabaseStorageRepository.findDetail(1L)).willReturn(Map.of(
                "authMethod", DatabaseAuthMethod.PASSWORD.name(),
                "username", "tester",
                "passwordEncrypted", encryptedPassword
        ));

        DatabaseStoredCredentialDecryptRequest request = new DatabaseStoredCredentialDecryptRequest();
        request.setRevealRaw(false);

        var response = queryService.decryptStoredCredentials(1L, request, "127.0.0.1");

        assertThat(response.getPasswordMasked()).isEqualTo("pw******et");
        assertThat(response.getUsernameMasked()).isEqualTo("te******er");
        assertThat(response.isIncludeRaw()).isFalse();
        verify(historyRepository).insert(1L, "DECRYPT", true, "복호화 조회(마스킹)", "requester=127.0.0.1");
    }

    @Test
    void decryptStoredCredentialsUsesAppCredentialKeyFallbackForCertificateMetaReveal() {
        String encryptedPassword = credentialEncryptor.encrypt("db-password", null);
        DatabaseCertificateAuthRequest certificateAuthRequest = new DatabaseCertificateAuthRequest();
        certificateAuthRequest.setTrustStorePath("/secure/truststore.jks");
        certificateAuthRequest.setTrustStorePassword("trust-pass");
        certificateAuthRequest.setKeyStorePath("/secure/client.p12");
        certificateAuthRequest.setKeyStorePassword("key-pass");
        certificateAuthRequest.setKeyAlias("client");
        certificateAuthRequest.setSslMode("verify-full");
        String metaJson = certificateMetaService.buildCredentialMetaJson(certificateAuthRequest, credentialEncryptor, null);

        given(externalDatabaseStorageRepository.findDetail(2L)).willReturn(Map.of(
                "authMethod", DatabaseAuthMethod.CERTIFICATE.name(),
                "username", "cert-user",
                "passwordEncrypted", encryptedPassword,
                "credentialMetaJson", metaJson
        ));
        given(credentialRevealService.consumeIfValid(2L, "token-1", "127.0.0.1")).willReturn(true);
        given(credentialRevealService.summarize("token-1")).willReturn("to***n1");

        DatabaseStoredCredentialDecryptRequest request = new DatabaseStoredCredentialDecryptRequest();
        request.setRevealRaw(true);
        request.setRevealToken("token-1");

        var response = queryService.decryptStoredCredentials(2L, request, "127.0.0.1");

        assertThat(response.isIncludeRaw()).isTrue();
        assertThat(response.getPassword()).isEqualTo("db-password");
        assertThat(response.getTrustStorePassword()).isEqualTo("trust-pass");
        assertThat(response.getKeyStorePassword()).isEqualTo("key-pass");
        assertThat(response.getKeyAlias()).isEqualTo("client");
        assertThat(response.getSslMode()).isEqualTo("verify-full");
        verify(historyRepository).insert(2L, "DECRYPT", true, "복호화 조회(평문)", "requester=127.0.0.1, token=to***n1");
    }
}
