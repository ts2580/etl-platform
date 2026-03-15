package com.etl.sfdc.user.model.service;

import com.etl.sfdc.user.model.dto.Member;
import com.etl.sfdc.user.model.repository.UserRepository;
import com.etlplatform.common.error.AppException;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "true")
public class UserServiceImpl implements UserService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    public Member getUserDes(String userName) {
        Member member = userRepository.getUserDes(userName);
        member = member == null ? new Member() : member;
        return member;
    }

    public Member create(String name, String username, String email, String password, String description) {
        String normalizedName = name == null ? null : name.trim();
        String normalizedUsername = username == null ? null : username.trim();
        String normalizedEmail = email == null ? null : email.trim();

        if (normalizedName == null || normalizedName.isBlank()) {
            throw new AppException("이름을 입력해 주세요.");
        }
        if (normalizedUsername == null || normalizedUsername.isBlank()) {
            throw new AppException("사용자명을 입력해 주세요.");
        }
        if (normalizedEmail == null || normalizedEmail.isBlank()) {
            throw new AppException("이메일을 입력해 주세요.");
        }
        if (password == null || password.isBlank()) {
            throw new AppException("비밀번호를 입력해 주세요.");
        }
        if (userRepository.findByUsername(normalizedUsername).isPresent()) {
            throw new AppException("이미 존재하는 사용자명입니다.");
        }
        if (userRepository.findByEmail(normalizedEmail).isPresent()) {
            throw new AppException("이미 사용 중인 이메일입니다.");
        }

        Member member = new Member();
        member.setName(normalizedName);
        member.setUsername(normalizedUsername);
        member.setEmail(normalizedEmail);
        member.setDescription(description == null ? null : description.trim());
        member.setPassword(passwordEncoder.encode(password));

        try {
            this.userRepository.create(member);
        } catch (DataAccessException e) {
            throw new AppException("회원 정보 저장 중 DB 오류가 발생했습니다. config.member 테이블과 DB 연결 설정을 확인해 주세요.", e);
        }
        return member;
    }
}
