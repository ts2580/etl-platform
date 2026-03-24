package com.etlplatform.common.storage.database;

import com.etlplatform.common.error.AppException;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

public class DatabaseCredentialEncryptor {

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int IV_LENGTH = 12;
    private static final int SALT_LENGTH = 16;
    private static final int TAG_LENGTH = 128;
    private static final int ITERATIONS = 65536;
    private static final int KEY_LENGTH = 256;
    private static final String VERSION = "v1";
    private static final String ALGORITHM = "gcm";
    private static final String ENV_KEY = "APP_DB_CREDENTIAL_KEY";

    private final SecureRandom secureRandom = new SecureRandom();

    public String encrypt(String rawValue, String userKey) {
        if (rawValue == null || rawValue.isBlank()) {
            return null;
        }

        String resolvedKey = resolveKey(userKey);
        byte[] salt = randomBytes(SALT_LENGTH);
        byte[] iv = randomBytes(IV_LENGTH);

        try {
            SecretKey secretKey = deriveKey(resolvedKey, salt);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(TAG_LENGTH, iv));
            byte[] encrypted = cipher.doFinal(rawValue.getBytes(StandardCharsets.UTF_8));
            return String.join(":",
                    VERSION,
                    ALGORITHM,
                    Base64.getEncoder().encodeToString(salt),
                    Base64.getEncoder().encodeToString(iv),
                    Base64.getEncoder().encodeToString(encrypted));
        } catch (Exception e) {
            throw new AppException("자격증명 암호화에 실패했어요.", e);
        }
    }

    public DecryptedDatabaseCredential decrypt(String encryptedValue, String userKey) {
        return new DecryptedDatabaseCredential(decryptToString(encryptedValue, userKey));
    }

    public String decryptToString(String encryptedValue, String userKey) {
        if (encryptedValue == null || encryptedValue.isBlank()) {
            return null;
        }

        String resolvedKey = resolveKey(userKey);

        try {
            String[] tokens = encryptedValue.split(":", 5);
            if (tokens.length != 5 || !VERSION.equals(tokens[0]) || !ALGORITHM.equals(tokens[1])) {
                throw new AppException("지원하지 않는 암호화 포맷입니다.");
            }

            byte[] salt = Base64.getDecoder().decode(tokens[2]);
            byte[] iv = Base64.getDecoder().decode(tokens[3]);
            byte[] cipherBytes = Base64.getDecoder().decode(tokens[4]);

            SecretKey secretKey = deriveKey(resolvedKey, salt);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(TAG_LENGTH, iv));
            return new String(cipher.doFinal(cipherBytes), StandardCharsets.UTF_8);
        } catch (AppException e) {
            throw e;
        } catch (Exception e) {
            throw new AppException("자격증명 복호화에 실패했어요. APP_DB_CREDENTIAL_KEY 설정을 확인해 주세요.", e);
        }
    }

    private SecretKey deriveKey(String key, byte[] salt) throws Exception {
        PBEKeySpec keySpec = new PBEKeySpec(key.toCharArray(), salt, ITERATIONS, KEY_LENGTH);
        SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
        byte[] keyBytes = factory.generateSecret(keySpec).getEncoded();
        return new SecretKeySpec(keyBytes, "AES");
    }

    private String resolveKey(String userKey) {
        String candidate = normalize(userKey);
        if (candidate != null) {
            validateResolvedKey(candidate, "요청 키");
            return candidate;
        }

        String envKey = normalize(System.getenv(ENV_KEY));
        if (envKey == null) {
            envKey = normalize(System.getProperty(ENV_KEY));
        }
        if (envKey == null) {
            throw new AppException("DB 자격증명 암호화 키가 비어 있습니다. 환경 변수 APP_DB_CREDENTIAL_KEY를 설정해 주세요.");
        }
        validateResolvedKey(envKey, ENV_KEY);
        return envKey;
    }

    private void validateResolvedKey(String key, String source) {
        if (key.length() < 8) {
            throw new AppException(source + "는 8자 이상이어야 해요.");
        }
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private byte[] randomBytes(int size) {
        byte[] bytes = new byte[size];
        secureRandom.nextBytes(bytes);
        return bytes;
    }
}
