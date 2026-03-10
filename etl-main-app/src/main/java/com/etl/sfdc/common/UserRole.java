package com.etl.sfdc.common;

import lombok.Getter;

@Getter
public enum UserRole {

    // 권한 부여시 사용하는 권한 집합

    ADMIN("ROLE_ADMIN"),
    USER("ROLE_USER");

    UserRole(String value) {
        this.value = value;
    }

    private String value;

}
