package com.etl.sfdc.common;

import jakarta.servlet.ServletException;
import org.springframework.security.web.session.SessionInformationExpiredEvent;
import org.springframework.security.web.session.SessionInformationExpiredStrategy;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class UserSessionExpiredStrategy implements SessionInformationExpiredStrategy {

    @Override
    public void onExpiredSessionDetected(SessionInformationExpiredEvent event) throws IOException, ServletException {

        // SessionInformationExpiredStrategy를 구현한 클래스. 세션 만료시 행할 정책 구현
        // 1. 중복로그인으로 인한 세션 만료시 정책을 구현

        // UserDetails의 구현체(dto에 있는 UserAccount)에 equals와 hashCode 메소드를 오버라이드해야 함
        // 이 메소드들은 사용자의 고유 식별자를 기반으로 해야 하며, 이는 UserDetails 객체 간의 동등성 비교를 위해 사용됨.

        System.out.println("세션만료");

        // 세션 만료 시 리다이렉트할 URL
        String redirectUrl = "/user/login?error=session";

        event.getResponse().sendRedirect(redirectUrl);
    }
}
