package com.apache.sfdc.common;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;

import java.util.*;

public class ArrayListAggregationStrategy implements AggregationStrategy {
    @Override
    public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
        //Exception in thread "Camel (camel-2) thread #7 - Aggregator" java.lang.StackOverflowError
        // 여기서 잘 처리해줘야댐 - 문서 : https://camel.apache.org/components/4.4.x/eips/aggregate-eip.html

        // OldExchange와 NewExchange를 하나의 Exchange로 집계하는데 사용된다.
        // newExchange는 다음번에는 oldExchange가 된다.

        // created, updated, deleted -> CUD 타입을 알 수 있음
        // cud에 대한 정보는 헤더에 담겨움
        String type = (String) newExchange.getIn().getHeader("CamelSalesforceEventType");

        Object newBody = newExchange.getIn().getBody();

        System.out.println("집계 시작!!");

        // 반환할 타입
        Map<String, List<Object>> mapReturn;
        if (oldExchange == null) {

            System.out.println("첫 집계==================");
            System.out.println(type);

            // 밖에서 선언하고 안에서 초기화 하는것은 최종 반환할 타입 뿐
            List<Object> list = new ArrayList<>();
            list.add(newBody);

            // 맨 처음 메세지가 들어왔을 때
            mapReturn = new HashMap<>();
            mapReturn.put(type, list);
            // setBody를 해주는 이유는 처음 들어온 경우 body 에 아무것도 없으니까 최종 반환할 타입 추가하기
            // setBody에는 최종 반환할 것을 담아준다.
            newExchange.getIn().setBody(mapReturn);
            return newExchange;
        } else {

            System.out.println("계속 들어옴=============");
            System.out.println("type ====> " + type);

            // oldExchange.setBody 할 필요없이 자동으로 oldExchange의 Body는 업데이트 된다.
            mapReturn = oldExchange.getIn().getBody(Map.class);
            List<Object> listTemp = mapReturn.get(type) != null ? mapReturn.get(type) : new ArrayList<>();
            listTemp.add(newBody);
            mapReturn.put(type, listTemp);

            // 여기서 setBody를 안하는 이유: 이미 같은 주소의 list가 body에 담긴 상태이므로 같은 주소를 참조중이다.
            // 따라서 list에 add를 하는 순간 oldExchange에 있는 list에도 자동으로 add 된다.

            return oldExchange;
        }
    }
}
