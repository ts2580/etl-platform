package com.sfdcupload.common;

import com.etlplatform.common.salesforce.SalesforceOAuthClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SalesforceOAuthClientConfig {

    @Bean
    public SalesforceOAuthClient salesforceOAuthClient() {
        return new SalesforceOAuthClient();
    }
}
