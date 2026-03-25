package com.apache.sfdc;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "app.db.enabled=false")
class SfdcApplicationTests {

    @Test
    void contextLoads() {
    }

}
