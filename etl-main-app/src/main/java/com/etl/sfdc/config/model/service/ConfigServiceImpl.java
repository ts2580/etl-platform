package com.etl.sfdc.config.model.service;

import com.etl.sfdc.config.model.dto.User;
import com.etl.sfdc.config.model.repository.ConfigRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ConfigServiceImpl implements ConfigService{

    private final ConfigRepository configRepository;

    public User getUserDes(String userName) {

        User user = configRepository.getUserDes(userName);
        //이놈의 삼항 자바에선 삼항쓰지맙시다!!
        user = user == null ? new User() : user;

        return user;
    }
}
















