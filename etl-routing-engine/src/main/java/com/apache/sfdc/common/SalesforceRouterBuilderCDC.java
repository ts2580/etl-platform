package com.apache.sfdc.common;

import com.apache.sfdc.pubsub.repository.PubSubRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.builder.RouteBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class SalesforceRouterBuilderCDC extends RouteBuilder {
    private final String selectedObject;
    private final Map<String, Object> mapType;
    private final PubSubRepository pubSubRepository;
    private final SalesforceCdcPayloadMapper payloadMapper = new SalesforceCdcPayloadMapper();
    private final SalesforceRecordMutationProcessor mutationProcessor = new SalesforceRecordMutationProcessor();

    public SalesforceRouterBuilderCDC(String selectedObject, Map<String, Object> mapType, PubSubRepository pubSubRepository) {
        this.selectedObject = selectedObject;
        this.mapType = mapType;
        this.pubSubRepository = pubSubRepository;
    }

    @Override
    public void configure() throws Exception {
        SqlSanitizer.validateTableName(selectedObject);

        String eventName = selectedObject.contains("__c")
                ? selectedObject.replace("__c", "__ChangeEvent")
                : selectedObject + "ChangeEvent";

        SalesforceMutationRepositoryPort repositoryPort = new SalesforceMutationRepositoryPort() {
            @Override
            public int insertObject(String upperQuery, List<String> listUnderQuery, String tailQuery) {
                return pubSubRepository.insertObject(upperQuery, listUnderQuery, tailQuery);
            }

            @Override
            public int updateObject(StringBuilder strUpdate) {
                return pubSubRepository.updateObject(strUpdate);
            }

            @Override
            public int deleteObject(StringBuilder strDelete) {
                return pubSubRepository.deleteObject(strDelete);
            }
        };

        from("sf:pubSubSubscribe:/data/" + eventName)
                .aggregate(constant(true), new ArrayListAggregationStrategy())
                .completionInterval(5000)
                .process((exchange) -> {
                    Map<String, List<Object>> messageBodies = exchange.getIn().getBody(Map.class);
                    if (messageBodies == null || messageBodies.isEmpty()) {
                        return;
                    }

                    List<Object> payloads = new ArrayList<>();
                    for (List<Object> bodies : messageBodies.values()) {
                        if (bodies != null && !bodies.isEmpty()) {
                            payloads.addAll(bodies);
                        }
                    }

                    int updatedCount = 0;
                    int insertedCount = 0;
                    int deletedCount = 0;

                    for (Object body : payloads) {
                        var mutationOptional = payloadMapper.map(body, mapType);
                        if (mutationOptional.isEmpty()) {
                            continue;
                        }

                        SalesforceRecordMutationProcessor.MutationResult result = mutationProcessor.apply(
                                selectedObject,
                                mapType,
                                mutationOptional.get(),
                                repositoryPort,
                                "CDC"
                        );
                        updatedCount += result.updated();
                        insertedCount += result.inserted();
                        deletedCount += result.deleted();
                    }

                    log.info("[CDC] updated={}, inserted={}, deleted={}, received={}", updatedCount, insertedCount, deletedCount, payloads.size());
                });
    }
}
