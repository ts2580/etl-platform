package com.etl.sfdc.user.model.dto;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
public class UserCreateForm {
    private String username;
    private String password1;
    private String password2;
    private String email;
    private String description;
}
