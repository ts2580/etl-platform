package com.etl.sfdc.home.model.dto;

import lombok.Data;
import org.springframework.security.core.userdetails.User;

@Data
public class UserAccount {
    private Integer id;
    private String name;
    private String username;
    private String password;
    private String email;
    private String description;
}
