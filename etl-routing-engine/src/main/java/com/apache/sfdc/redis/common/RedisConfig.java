package com.apache.sfdc.redis.common;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
    public class RedisConfig {

    @Value("${spring.data.redis.host}")
    private String host;

    @Value("${spring.data.redis.port}")
    private int port;

    @Value("${spring.data.redis.password}")
    private String password;

    /**
     * Redis 와의 연결을 위한 'Connection'을 생성.
     * 헤로쿠 레디스는 기본적으로 비번이 걸려있기 때문에 return new LettuceConnectionFactory(host, port); 사용 못함
     * 한참 헤멨네
     */
    @Bean
    public RedisConnectionFactory redisConnectionFactory() {

        // RedisConnectionFactory는 인터페이스 , RedisStandaloneConfiguration는 저 인터페이스를 구현한 구현체임
        RedisStandaloneConfiguration redisStandaloneConfiguration = new RedisStandaloneConfiguration();
        redisStandaloneConfiguration.setHostName(host);
        redisStandaloneConfiguration.setPort(port);
        redisStandaloneConfiguration.setPassword(password);

        // SSL 사용
        // disablePeerVerification : 피어 검증 안함.
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .useSsl()
                .disablePeerVerification()
                .build();

        // LettuceConnectionFactory : RedisConnectionFactory 인터페이스를 구현한 클래스. 그래서 리턴값도 RedisConnectionFactory
        //                            내장 되어 있는 Lettuce 라이브러리를 사용하여 Redis 커넥션을 생성하고 관리하는데 사용
        
        return new LettuceConnectionFactory(redisStandaloneConfiguration);
    }

    /**
     * Redis 데이터 처리를 위한 템플릿을 구성
     * Redis 데이터베이스에 접근하고 데이터를 처리하는 데 사용
     * 타입 안전성을 제공하고, 직렬화 및 역직렬화를 처리하며, Redis의 기본적인 데이터 구조를 지원
     */
    @Bean
    public RedisTemplate<String, Object> redisTemplate() {
        RedisTemplate<String, Object> redisTemplate = new RedisTemplate<>();

        // Redis를 연결
        redisTemplate.setConnectionFactory(redisConnectionFactory());

        // Key-Value 형태로 직렬화를 수행
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new StringRedisSerializer());

        // Hash Key-Value 형태로 직렬화를 수행
        redisTemplate.setHashKeySerializer(new StringRedisSerializer());
        redisTemplate.setHashValueSerializer(new StringRedisSerializer());

        // 기본적으로 직렬화를 수행
        redisTemplate.setDefaultSerializer(new StringRedisSerializer());

        return redisTemplate;
    }
}