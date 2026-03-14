package com.etl.sfdc.config.controller;

import com.etl.sfdc.config.model.dto.User;
import com.etl.sfdc.config.model.service.ConfigService;
import com.etlplatform.common.validation.RequestValidationUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("config")
@Slf4j
public class ConfigController {

    private final ConfigService configService;

    @PostMapping("/user")
    public ResponseEntity<User> hello(@RequestBody Map<String, Object> map) {
        String userName = RequestValidationUtils.requireText(String.valueOf(map.get("userName")), "userName");
        log.info("Loading config for userName={}", userName);
        User user = configService.getUserDes(userName);
        return ResponseEntity.ok(user);
    }
}
