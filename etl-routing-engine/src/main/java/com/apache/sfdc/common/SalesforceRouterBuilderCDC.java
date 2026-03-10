package com.apache.sfdc.common;

import com.apache.sfdc.pubsub.repository.PubSubRepository;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;

public class SalesforceRouterBuilderCDC extends RouteBuilder {
    private final String selectedObject;
    private final PubSubRepository pubSubRepository;

    public SalesforceRouterBuilderCDC(String selectedObject, PubSubRepository pubSubRepository) {
        this.selectedObject = selectedObject;
        this.pubSubRepository = pubSubRepository;
    }

    @Override
    public void configure() throws Exception {

        // std obj는 뒤에 곧바로 ChangeEvent 붙이고, custom은 __c를 __ChangeEvent로 교체.
        String eventName = selectedObject.contains("__c") ? selectedObject.replace("__c", "__ChangeEvent") : selectedObject + "ChangeEvent";

        // Redis로 메시지 날릴것 아니고 Failover용 보조 DB로 쓸꺼니 Camel은 쓰지 말자

        from("sf:pubSubSubscribe:/data/" + eventName)
                .process((exchange) -> {
                    Message message = exchange.getIn();
                    System.out.println(message.getHeader("CamelSalesforcePubSubReplayId"));
                    System.out.println(message.getBody());

                });
    }
}
