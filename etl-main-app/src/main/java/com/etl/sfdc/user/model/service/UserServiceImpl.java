package com.etl.sfdc.user.model.service;

import com.etl.sfdc.user.model.dto.Member;
import com.etl.sfdc.user.model.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class UserServiceImpl implements UserService{

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    public Member getUserDes(String userName) {
        Member member = userRepository.getUserDes(userName);

        member = member == null ? new Member() : member;

        return member;
    }

    public Member create(String username, String email, String password, String description) {
        Member member = new Member();
        member.setUsername(username);
        member.setName(username);
        member.setEmail(email);
        member.setDescription(description);
        member.setPassword(passwordEncoder.encode(password));
        this.userRepository.create(member);
        return member;
    }
}
