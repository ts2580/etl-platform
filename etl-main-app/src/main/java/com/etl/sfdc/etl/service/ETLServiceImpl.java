package com.etl.sfdc.etl.service;

import com.etl.sfdc.etl.dto.ObjectDefinition;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class ETLServiceImpl implements ETLService {

    @Value("${salesforce.myDomain}")
    private String myDomain;

    @Value(("${aws.albUri}"))
    private String albUri;

    @Override
    public List<ObjectDefinition> getObjects(String accessToken) throws Exception {

        List<ObjectDefinition> listDef = new ArrayList<>();

        OkHttpClient client = new OkHttpClient();

        Request request = new Request.Builder()
                .url(myDomain + "/services/data/v63.0/sobjects")
                .addHeader("Authorization", "Bearer " + accessToken)
                .addHeader("Content-Type", "application/json")
                .build();

        try(Response response = client.newCall(request).execute()) {
            if (response.isSuccessful()) {
                String responseBody = response.body().string();

                System.out.println("responseBody ==> " + responseBody);

                // 잭슨으로 역직렬화
                ObjectMapper objectMapper = new ObjectMapper();

                // 세일즈 포스로 따지면 JSON.deserializeUntyped();
                JsonNode rootNode = objectMapper.readTree(responseBody);

                JsonNode sobjects = rootNode.get("sobjects");

                listDef = objectMapper.convertValue(sobjects, new TypeReference<List<ObjectDefinition>>(){});

            } else {
                System.err.println("오브젝트 목록 불러오기 실패 : " + response.message());
            }
        }catch (IOException e) {
            System.out.println(e.getMessage());
        }

        return listDef;
    }

    @Override
    public void setObjects(String selectedObject, String accessToken, String refreshToken) throws Exception {

        ObjectMapper objectMapper = new ObjectMapper();

        OkHttpClient client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(getProprtyMap(selectedObject, accessToken, refreshToken));

        System.out.println(json);

        // Map 만들고 잭슨으로 직렬화
        RequestBody formBody = RequestBody.create(
                json, MediaType.get("application/json; charset=utf-8")
        );

        // x-www-form-urlencoded 말고 얌전히 json 보내자
        // 도커 네트워크 대역으로. 게이트웨이에 요청 보낸다.
        Request request = new Request.Builder()
                .url(albUri + ":3931/streaming")
                .post(formBody)
                .addHeader("Content-Type", "application/json")
                .build();

        try(Response response = client.newCall(request).execute()) {

            System.out.println(Objects.requireNonNull(response.body()).string());

        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

    private static @NotNull Map<String, String> getProprtyMap(String selectedObject, String accessToken, String refreshToken) {
        Map<String, String> mapProperty = new HashMap<>();
        mapProperty.put("selectedObject", selectedObject);
        mapProperty.put("accessToken", accessToken);
        mapProperty.put("refreshToken", refreshToken);

        return mapProperty;
    }
}