package com.etl.sfdc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class ETLApplication {

    public static void main(String[] args) {

        // Heroku가 자동으로 주입하는 Heroku PG의 DATABASE_URL이 자꾸 DB로 들어와서 에러냄.
        // 이를 덮어쓰기 위해서 DATABASE_URL을 DB_URL로 강제 override.

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
