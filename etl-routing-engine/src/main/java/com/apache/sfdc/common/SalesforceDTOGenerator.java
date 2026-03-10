package com.apache.sfdc.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

@SpringBootApplication
public class SalesforceDTOGenerator {

    private static final String INSTANCE_URL = "https://posco--scrum4.sandbox.my.salesforce.com/";

    public static void main(String[] args) throws Exception {

        // 단순 요청 보내기엔 OkHttp가 제일 좋은것 같음.
        // 아직 Non-Block I/O가 필요한건 아니기에 Spring WebClient는 잠시 봉인
        OkHttpClient client = new OkHttpClient();

        String sObjectName = "AllFieldType__c"; // SObject 이름
        Request request = new Request.Builder()
                .url(INSTANCE_URL + "/services/data/v60.0/sobjects/" + sObjectName + "/describe")
                .addHeader("Authorization", "Bearer " + SalesforceOAuth.getAccessToken())
                .build();

        Response response = client.newCall(request).execute();

        if (response.isSuccessful()) {
            String responseBody = response.body().string();

            // 잭슨으로 역직렬화
            ObjectMapper objectMapper = new ObjectMapper();

            // 세일즈 포스로 따지면 JSON.deserializeUntyped();
            JsonNode rootNode = objectMapper.readTree(responseBody);
            generateDTOClass(rootNode, sObjectName);
        } else {
            System.err.println("Failed to retrieve metadata: " + response.message());
        }
    }

    private static void generateDTOClass(JsonNode rootNode, String sObjectName) throws FileNotFoundException {
        StringBuilder classBuilder = new StringBuilder();
        classBuilder.append("package com.apache.sfdc.router.dto;\n\n");
        classBuilder.append("import lombok.Data;\n");
        classBuilder.append("import java.sql.Time;\n");
        classBuilder.append("import java.time.LocalDate;\n");
        classBuilder.append("import java.time.LocalDateTime;\n\n");
        classBuilder.append("@Data \n");
        classBuilder.append("public class ").append(sObjectName).append(" {\n");

        JsonNode fields = rootNode.get("fields");
        for (JsonNode field : fields) {
            String fieldType = field.get("type").asText();
            String javaType = mapToJavaType(fieldType);
            String fieldName = field.get("name").asText();
            String fieldLabel = field.get("label").asText();

            classBuilder
                    .append(" private ")
                    .append(javaType)
                    .append(" ")
                    .append(fieldName)
                    .append("; // ")
                    .append(fieldLabel)
                    .append("\n");
        }

        classBuilder.append("}\n");

        // 출력 디렉토리 생성
        File dir = new File("src/main/java/com/apache/sfdc/router/dto");
        if (!dir.exists()) {
            dir.mkdirs();
        }

        // 파일 생성
        File file = new File(dir, sObjectName + ".java");
        try (PrintWriter out = new PrintWriter(file)) {
            out.print(classBuilder);
            System.out.println("DTO class 경로: " + file.getAbsolutePath());
        }
    }

    private static String mapToJavaType(String fieldType) {

        // 세일즈포스 타입을 자바 타입으로 쪼개기. 날짜 관련 필드는 나중에 무결성 확인 요
        return switch (fieldType) {
            case "boolean" -> "Boolean";
            case "date" -> "LocalDate";
            case "datetime" -> "LocalDateTime";
            case "time" -> "Time";
            case "double", "currency", "percent" -> "Double";
            default -> "String";
        };
    }

}