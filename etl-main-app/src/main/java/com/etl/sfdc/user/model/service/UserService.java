package com.etl.sfdc.user.model.service;

import com.etl.sfdc.user.model.dto.Member;

public interface UserService {
    Member getUserDes(String userName);

    Member create(String username, String email, String password, String description);
}
