package com.etl.sfdc.user.model.service;

import com.etlplatform.common.error.FeatureDisabledException;
import com.etl.sfdc.user.model.dto.Member;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "app.db.enabled", havingValue = "false", matchIfMissing = true)
public class NoOpUserService implements UserService {
    @Override
    public Member getUserDes(String userName) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 사용자 조회를 사용할 수 없습니다.");
    }

    @Override
    public Member create(String username, String email, String password, String description) {
        throw new FeatureDisabledException("DB 비활성 모드에서는 회원가입을 사용할 수 없습니다.");
    }
}
