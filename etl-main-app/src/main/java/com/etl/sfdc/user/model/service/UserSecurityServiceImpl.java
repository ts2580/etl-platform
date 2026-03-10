package com.etl.sfdc.user.model.service;

import com.etl.sfdc.common.UserRole;
import com.etl.sfdc.common.UserSession;
import com.etl.sfdc.user.model.dto.Member;
import com.etl.sfdc.user.model.dto.UserAccount;
import com.etl.sfdc.user.model.repository.UserRepository;
import lombok.RequiredArgsConstructor;
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
public class UserSecurityServiceImpl implements UserSecurityService, UserDetailsService {

    private final UserRepository userRepository;

    private final UserSession userSession;

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        Optional<Member> _siteUser = this.userRepository.findByUsername(username);

        if (_siteUser.isEmpty()) {
            throw new UsernameNotFoundException("사용자를 찾을수 없습니다.");
        }

        // 일단 한성진 유저인 경우에 어드민 권한 부여
        List<GrantedAuthority> authorities = new ArrayList<>();
        if ("한성진".equals(username)) {
            authorities.add(new SimpleGrantedAuthority(UserRole.ADMIN.getValue()));
        } else {
            authorities.add(new SimpleGrantedAuthority(UserRole.USER.getValue()));
        }

        // Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        // User user = (User)authentication.getPrincipal();

        // Spring Security에서는 Session에서 현재 사용자의 정보를 Principal(Java 표준 객체)로 조회 가능한데
        // 이걸로 받을 수 있는 정보는 name뿐임. 하지만 내 DB에는 사용자에 대한 많은 정보가 있다.
        // 따라서 세션에 뿌려줄 객체인 UserAccount를 만든다.
        // 정보 객체로 사용되는 객체는 UserDetails를 구현하는 User를 상속받아야 한다.
        // loadUserByUsername의 return 타입이 UserDetails이니까!

        Member siteUser = _siteUser.get();
        siteUser.setAuthority(authorities);

        UserAccount userAccount = new UserAccount(siteUser);

        // 세션에 UserAccount 넣어주기
        userSession.setUserAccount(userAccount);

        // User를 상속받는 UserAccount를 통해 커스텀한 Principal을 반환

        return userAccount;
    }
}
