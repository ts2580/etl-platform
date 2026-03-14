package com.etl.sfdc.config.model.service;

import com.etl.sfdc.config.model.dto.User;
import com.etl.sfdc.config.model.repository.ConfigRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class ConfigServiceImpl implements ConfigService {

    private final ConfigRepository configRepository;

    public User getUserDes(String userName) {
        User user = configRepository.getUserDes(userName);
        user = user == null ? new User() : user;
        return user;
    }
}
