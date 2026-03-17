package com.etl.sfdc.common;

import com.etlplatform.common.salesforce.SalesforceClientCredentialsClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SalesforceClientCredentialsClientConfig {

    @Bean
    public SalesforceClientCredentialsClient salesforceOAuthClient() {
        return new SalesforceClientCredentialsClient();
    }
}
