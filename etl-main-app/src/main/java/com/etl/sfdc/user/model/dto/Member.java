package com.etl.sfdc.user.model.dto;

import lombok.Data;
import org.springframework.security.core.GrantedAuthority;

import java.util.List;

@Data
public class Member {
    private Integer id;
    private String name;
    private String username;
    private String password;
    private String email;
    private String description;
    private List<GrantedAuthority> authority;
}
