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
import org.springframework.security.authentication.InternalAuthenticationServiceException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class UserSecurityServiceImpl implements UserSecurityService, UserDetailsService {

    private final UserRepository userRepository;
    private final UserSession userSession;

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        String safeUsername = username == null ? "" : username.trim();
        if (safeUsername.isBlank()) {
            throw new UsernameNotFoundException("아이디가 비어 있습니다.");
        }

        try {
            Optional<Member> optionalSiteUser = this.userRepository.findByUsername(safeUsername);
            if (optionalSiteUser.isEmpty()) {
                throw new UsernameNotFoundException("사용자를 찾을 수 없습니다.");
            }

            List<GrantedAuthority> authorities = new ArrayList<>();
            if ("한성진".equals(safeUsername)) {
                authorities.add(new SimpleGrantedAuthority(UserRole.ADMIN.getValue()));
            } else {
                authorities.add(new SimpleGrantedAuthority(UserRole.USER.getValue()));
            }

            Member siteUser = optionalSiteUser.get();
            siteUser.setAuthority(authorities);

            String principal = siteUser.getUsername() != null ? siteUser.getUsername() : safeUsername;
            String password = siteUser.getPassword();
            if (password == null || password.isBlank()) {
                throw new InternalAuthenticationServiceException("회원 비밀번호가 비정상입니다.");
            }

            Member safeSiteUser = new Member();
            safeSiteUser.setId(siteUser.getId());
            safeSiteUser.setUsername(principal);
            safeSiteUser.setName((siteUser.getName() == null || siteUser.getName().isBlank()) ? principal : siteUser.getName());
            safeSiteUser.setDescription(siteUser.getDescription());
            safeSiteUser.setPassword(password);
            safeSiteUser.setEmail(siteUser.getEmail() != null ? siteUser.getEmail() : principal + "@local");
            safeSiteUser.setAuthority(authorities);

            UserAccount userAccount = new UserAccount(safeSiteUser);
            userSession.setUserAccount(userAccount);
            return userAccount;
        } catch (UsernameNotFoundException e) {
            throw e;
        } catch (Exception e) {
            log.error("loadUserByUsername failed. username={}", safeUsername, e);
            throw new InternalAuthenticationServiceException("사용자 인증 처리 중 오류가 발생했습니다.", e);
        }
    }
}
