package com.etl.sfdc.storage.service;

import com.etl.sfdc.storage.dto.DecryptedDatabaseCredential;
import com.etl.sfdc.storage.support.DatabaseCredentialEncryptor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DatabaseCredentialDecryptService {

    private final DatabaseCredentialEncryptor credentialEncryptor;

    public DecryptedDatabaseCredential decryptPassword(String encryptedPassword, String userKey) {
        com.etlplatform.common.storage.database.DecryptedDatabaseCredential decrypted = credentialEncryptor.decrypt(encryptedPassword, userKey);
        return DecryptedDatabaseCredential.builder().password(decrypted.password()).build();
    }
}
