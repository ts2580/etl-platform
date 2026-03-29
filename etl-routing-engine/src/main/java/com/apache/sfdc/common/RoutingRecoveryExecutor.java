package com.apache.sfdc.common;

import java.util.Map;

public interface RoutingRecoveryExecutor {
    String protocol();

    int recover(Map<String, String> mapProperty, String actor) throws Exception;
}
