package com.etl.sfdc.common;

import com.etl.sfdc.common.UserSessionExpiredStrategy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.core.session.SessionRegistryImpl;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;


@Configuration                  // 스프링 환경 설정 파일
@EnableWebSecurity              // 모든 요청 URL이 Spring Security의 제어를 받도록 함
public class SecurityConfig {

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {

        // Spring Security의 세부 설정은 @Bean 을 통해 SecurityFilterChain Bean을 생성하여 설정 가능
        // new AntPathRequestMatcher("/**")).permitAll()) <- 일단 로그인 없이 모든 페이지에 접근 가능하게 함
        // formLogin에서 로그인 관련 처리. loginPage에서 지정한 페이지가 로그인 페이지로 지정.
        // sessionManagement에서 세션 정책 처리. 커스텀 정책인 userSessionExpiredStrategy로 보냄
        //  sessionRegistry를 해야 세션사용자(SessionInformation)를 모니터링 가능

        http
                .authorizeHttpRequests((authorizeHttpRequests) -> authorizeHttpRequests
                        .requestMatchers(new AntPathRequestMatcher("/**")).permitAll())
                .formLogin((formLogin) -> formLogin
                        .loginPage("/user/login")
                        .defaultSuccessUrl("/")
                        .failureUrl("/user/login?error=login"))
                .logout((logout) -> logout
                        .logoutRequestMatcher(new AntPathRequestMatcher("/user/logout"))
                        .logoutSuccessUrl("/")
                        .invalidateHttpSession(true))
                .sessionManagement((sessionManagement) -> sessionManagement
                        .sessionConcurrency((concurrencyControl) -> concurrencyControl
                        .maximumSessions(1)
                        .maxSessionsPreventsLogin(false)
                        .expiredSessionStrategy(new UserSessionExpiredStrategy())
                        .sessionRegistry(sessionRegistry())))
        ;
        return http.build();
    }

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    @Bean
    public SessionRegistry sessionRegistry() {
        return new SessionRegistryImpl();
    }

    @Bean
    AuthenticationManager authenticationManager(AuthenticationConfiguration authenticationConfiguration) throws Exception {
        return authenticationConfiguration.getAuthenticationManager();
    }
}
