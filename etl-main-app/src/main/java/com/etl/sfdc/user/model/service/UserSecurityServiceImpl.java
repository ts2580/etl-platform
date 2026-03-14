package com.etl.sfdc.user.model.service;

import com.etl.sfdc.common.UserRole;
import com.etl.sfdc.common.UserSession;
import com.etl.sfdc.user.model.dto.Member;
import com.etl.sfdc.user.model.dto.UserAccount;
import com.etl.sfdc.user.model.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class UserSecurityServiceImpl implements UserSecurityService, UserDetailsService {

    private final UserRepository userRepository;
    private final UserSession userSession;

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        Optional<Member> _siteUser = this.userRepository.findByUsername(username);
        if (_siteUser.isEmpty()) {
            throw new UsernameNotFoundException("사용자를 찾을수 없습니다.");
        }

        List<GrantedAuthority> authorities = new ArrayList<>();
        if ("한성진".equals(username)) {
            authorities.add(new SimpleGrantedAuthority(UserRole.ADMIN.getValue()));
        } else {
            authorities.add(new SimpleGrantedAuthority(UserRole.USER.getValue()));
        }

        Member siteUser = _siteUser.get();
        siteUser.setAuthority(authorities);
        UserAccount userAccount = new UserAccount(siteUser);
        userSession.setUserAccount(userAccount);
        return userAccount;
    }
}
