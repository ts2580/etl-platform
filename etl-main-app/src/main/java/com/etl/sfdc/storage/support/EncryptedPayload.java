package com.etl.sfdc.storage.support;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class EncryptedPayload {
    private final byte[] salt;
    private final byte[] iv;
    private final byte[] cipherBytes;
}
