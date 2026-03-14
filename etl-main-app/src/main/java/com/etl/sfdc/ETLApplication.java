package com.etl.sfdc;

import org.mybatis.spring.boot.autoconfigure.MybatisAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication(exclude = {
        DataSourceAutoConfiguration.class,
        DataSourceTransactionManagerAutoConfiguration.class,
        MybatisAutoConfiguration.class
})
public class ETLApplication {

    public static void main(String[] args) {

        String dbUrl = System.getenv("DB_URL");
        String dbUsername = System.getenv("DB_USERNAME");
        String dbPassword = System.getenv("DB_PASSWORD");

        if (dbUrl != null) {
            System.setProperty("spring.datasource.url", dbUrl);
        }
        if (dbUsername != null) {
            System.setProperty("spring.datasource.username", dbUsername);
        }
        if (dbPassword != null) {
            System.setProperty("spring.datasource.password", dbPassword);
        }

        SpringApplication.run(ETLApplication.class, args);
    }
}
