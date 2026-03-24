package com.etl.sfdc.common;

import com.etlplatform.common.error.ApiErrorResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.session.SessionRegistry;
import org.springframework.security.core.session.SessionRegistryImpl;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.LoginUrlAuthenticationEntryPoint;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

@Configuration
@EnableWebSecurity
@Slf4j
public class SecurityConfig {

    private static final List<String> PUBLIC_PATHS = List.of(
            "/",
            "/user/login",
            "/user/signup",
            "/css/**",
            "/js/**",
            "/images/**",
            "/favicon.ico",
            "/error"
    );
    private static final Pattern SENSITIVE_VALUE_CHARS = Pattern.compile("[^A-Za-z0-9]");

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {

        http
                .authorizeHttpRequests(authorize -> authorize
                        .requestMatchers(PUBLIC_PATHS.toArray(String[]::new)).permitAll()
                        .anyRequest().authenticated()
                )
                .formLogin(formLogin -> formLogin
                        .loginPage("/")
                        .loginProcessingUrl("/user/login")
                        .usernameParameter("id")
                        .defaultSuccessUrl("/")
                        .failureUrl("/?error=login")
                )
                .logout(logout -> logout
                        .logoutUrl("/user/logout")
                        .logoutSuccessUrl("/")
                        .invalidateHttpSession(true)
                )
                .sessionManagement(sessionManagement -> sessionManagement
                        .sessionConcurrency(concurrencyControl -> concurrencyControl
                                .maximumSessions(1)
                                .maxSessionsPreventsLogin(false)
                                .expiredSessionStrategy(new UserSessionExpiredStrategy())
                                .sessionRegistry(sessionRegistry())
                        )
                )
                .csrf(csrf -> csrf
                        .csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
                )
                .exceptionHandling(exceptionHandling -> exceptionHandling
                        .authenticationEntryPoint((request, response, authException) -> {
                            if (isStorageApiRequest(request.getRequestURI())) {
                                logSecurityFailure(request, "UNAUTHORIZED");
                                writeJsonError(response, HttpServletResponse.SC_UNAUTHORIZED,
                                        new ApiErrorResponse("UNAUTHORIZED", "Authentication required", "로그인이 필요해요. 다시 로그인한 뒤 시도해 주세요."));
                                return;
                            }
                            new LoginUrlAuthenticationEntryPoint("/").commence(request, response, authException);
                        })
                        .accessDeniedHandler((request, response, accessDeniedException) -> {
                            if (isStorageApiRequest(request.getRequestURI())) {
                                logSecurityFailure(request, "FORBIDDEN");
                                writeJsonError(response, HttpServletResponse.SC_FORBIDDEN,
                                        new ApiErrorResponse("FORBIDDEN", "Access denied", "이 요청을 처리할 권한이 없거나 보안 검증에 실패했어요. 페이지를 새로고침한 뒤 다시 시도해 주세요."));
                                return;
                            }
                            response.sendRedirect("/");
                        })
                );
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

    private boolean isStorageApiRequest(String uri) {
        return uri != null && uri.startsWith("/api/storages/");
    }

    private void writeJsonError(HttpServletResponse response, int status, ApiErrorResponse body) throws IOException {
        response.setStatus(status);
        response.setCharacterEncoding("UTF-8");
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        objectMapper.writeValue(response.getWriter(), body);
    }

    private void logSecurityFailure(jakarta.servlet.http.HttpServletRequest request, String reason) {
        log.warn("[SECURITY] {} request uri={} method={} csrfHeader={} csrfCookie={}",
                reason,
                request.getRequestURI(),
                request.getMethod(),
                maskSensitiveValue(request.getHeader("X-CSRF-TOKEN")),
                maskSensitiveValue(readCookie(request, "XSRF-TOKEN")));
    }

    private String maskSensitiveValue(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String normalized = SENSITIVE_VALUE_CHARS.matcher(value).replaceAll("");
        if (normalized.isBlank()) {
            normalized = value;
        }
        if (normalized.length() <= 4) {
            return "****";
        }
        return normalized.substring(0, 2) + "***" + normalized.substring(normalized.length() - 2);
    }

    private String readCookie(jakarta.servlet.http.HttpServletRequest request, String name) {
        String cookie = request.getHeader("Cookie");
        if (cookie == null || cookie.isBlank()) {
            return null;
        }
        for (String raw : cookie.split(";")) {
            String token = raw.trim();
            if (token.startsWith(name + "=")) {
                return token.substring(name.length() + 1);
            }
        }
        return null;
    }
}
