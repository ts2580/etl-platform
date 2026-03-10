package com.etl.sfdc.config.model.dto;

import lombok.Data;

@Data
public class OAuth {

    // 세일즈포스 인증용. OAuth 2.0 인증시 사용되는 파리미터들.

    String grant_type;
    String client_id;
    String client_secret;
    String username;
    String password;
}
