package com.etl.sfdc.user.model.service;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpUserSecurityService implements UserSecurityService, UserDetailsService {
    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        throw new UsernameNotFoundException("DB 비활성 모드에서는 로그인 인증을 사용할 수 없습니다.");
    }
}
