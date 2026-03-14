package com.apache.sfdc.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@RequiredArgsConstructor
@Slf4j
public class SalesforceDTOGenerator {

    private static final String INSTANCE_URL = "https://posco--scrum4.sandbox.my.salesforce.com/";

    private final SalesforceOAuth salesforceOAuth;

    public void run() throws Exception {

        OkHttpClient client = new OkHttpClient();

        String sObjectName = "AllFieldType__c"; // SObject 이름
        String accessToken = salesforceOAuth.getAccessToken(createTokenMap());

        Request request = new Request.Builder()
                .url(INSTANCE_URL + "/services/data/v60.0/sobjects/" + sObjectName + "/describe")
                .addHeader("Authorization", "Bearer " + accessToken)
                .build();

        try (Response response = client.newCall(request).execute()) {

            if (response.isSuccessful()) {
                String responseBody = response.body().string();

                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode rootNode = objectMapper.readTree(responseBody);
                generateDTOClass(rootNode, sObjectName);
            } else {
                log.error("Failed to retrieve metadata: {}", response.message());
            }
        }
    }

    private Map<String, String> createTokenMap() {
        Map<String, String> mapProperty = new HashMap<>();
        mapProperty.put("loginUrl", INSTANCE_URL);
        mapProperty.put("client_id", System.getenv().getOrDefault("SALESFORCE_CLIENT_ID", ""));
        mapProperty.put("client_secret", System.getenv().getOrDefault("SALESFORCE_CLIENT_SECRET", ""));
        mapProperty.put("username", System.getenv().getOrDefault("SALESFORCE_USERNAME", ""));
        mapProperty.put("password", System.getenv().getOrDefault("SALESFORCE_PASSWORD", ""));
        return mapProperty;
    }

    public static void main(String[] args) throws Exception {
        try (ConfigurableApplicationContext context = SpringApplication.run(SalesforceDTOGenerator.class, args)) {
            context.getBean(SalesforceDTOGenerator.class).run();
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

        File dir = new File("src/main/java/com/apache/sfdc/router/dto");
        if (!dir.exists()) {
            dir.mkdirs();
        }

        File file = new File(dir, sObjectName + ".java");
        try (PrintWriter out = new PrintWriter(file)) {
            out.print(classBuilder);
            log.info("DTO class 경로: {}", file.getAbsolutePath());
        }
    }

    private static String mapToJavaType(String fieldType) {

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
