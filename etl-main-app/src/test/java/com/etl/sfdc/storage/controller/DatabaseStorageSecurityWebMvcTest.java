package com.etl.sfdc.storage.controller;

import com.etl.sfdc.common.SecurityConfig;
import com.etl.sfdc.storage.dto.DatabaseConnectionTestResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageDetailResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageListPageResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageListResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageRegistrationResponse;
import com.etl.sfdc.storage.service.DatabaseConnectionTestService;
import com.etl.sfdc.storage.service.DatabaseStorageManagementService;
import com.etl.sfdc.storage.service.DatabaseStorageQueryService;
import com.etl.sfdc.storage.service.DatabaseStorageRegistrationService;
import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etlplatform.common.storage.database.DatabaseVendor;
import com.etlplatform.common.storage.database.StorageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.model;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrlPattern;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.view;

@WebMvcTest(controllers = {
        StoragePageController.class,
        DatabaseStorageController.class,
        CsrfTokenController.class
})
@Import(SecurityConfig.class)
class DatabaseStorageSecurityWebMvcTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private DatabaseStorageRegistrationService registrationService;

    @MockBean
    private DatabaseConnectionTestService connectionTestService;

    @MockBean
    private DatabaseStorageQueryService queryService;

    @MockBean
    private DatabaseStorageManagementService managementService;

    @BeforeEach
    void setUp() {
        given(queryService.getList(anyInt(), anyInt(), anyString(), anyString())).willReturn(DatabaseStorageListPageResponse.builder()
                .page(0)
                .size(10)
                .totalElements(1)
                .totalPages(1)
                .items(List.of(DatabaseStorageListResponse.builder()
                        .id(1L)
                        .name("demo")
                        .storageType("DATABASE")
                        .vendor(DatabaseVendor.POSTGRESQL)
                        .authMethod(DatabaseAuthMethod.PASSWORD)
                        .jdbcUrl("localhost:5432/demo")
                        .username("tester")
                        .connectionStatus("SUCCESS")
                        .createdAt("2026-03-23T15:00:00")
                        .build()))
                .build());

        given(queryService.getDetail(anyLong())).willReturn(DatabaseStorageDetailResponse.builder()
                .id(1L)
                .name("demo")
                .description("desc")
                .storageType("DATABASE")
                .vendor(DatabaseVendor.POSTGRESQL)
                .authMethod(DatabaseAuthMethod.PASSWORD)
                .jdbcUrl("localhost:5432/demo")
                .port(5432)
                .username("tester")
                .schemaName("public")
                .databaseName("demo")
                .connectionStatus("SUCCESS")
                .createdAt("2026-03-23T15:00:00")
                .updatedAt("2026-03-23T15:00:00")
                .build());
    }

    @Test
    @WithMockUser(username = "tester")
    void managementPageRendersCsrfMetaTags() throws Exception {
        mockMvc.perform(get("/storages/databases"))
                .andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("meta name=\"csrf-token\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("meta name=\"csrf-header\"")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("/js/csrf-fetch.js")))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("decrypt-user-key"))));
    }

    @Test
    @WithMockUser(username = "tester")
    void pageRegisterSubmitUsesServerPostFlow() throws Exception {
        given(registrationService.register(any())).willReturn(DatabaseStorageRegistrationResponse.builder()
                .id(1L)
                .name("demo")
                .storageType(StorageType.DATABASE)
                .vendor(DatabaseVendor.POSTGRESQL)
                .authMethod(DatabaseAuthMethod.PASSWORD)
                .jdbcUrl("localhost:5432/demo")
                .username("tester")
                .connectionStatus("SUCCESS")
                .enabled(true)
                .build());

        mockMvc.perform(post("/storages/databases")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .param("name", "demo")
                        .param("encryptionKey", "secret-secret-secret")
                        .param("vendor", "POSTGRESQL")
                        .param("authMethod", "PASSWORD")
                        .param("jdbcUrl", "localhost:5432/demo")
                        .param("port", "5432")
                        .param("username", "tester")
                        .param("password", "pw"))
                .andExpect(status().isOk())
                .andExpect(view().name("database_storage_management"))
                .andExpect(model().attribute("resultTone", "success"));
    }

    @Test
    @WithMockUser(username = "tester")
    void csrfEndpointPublishesTokenAndCookie() throws Exception {
        mockMvc.perform(get("/api/csrf"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token").isNotEmpty())
                .andExpect(jsonPath("$.headerName").value("X-CSRF-TOKEN"))
                .andExpect(jsonPath("$.parameterName").value("_csrf"));
    }

    @Test
    void unauthenticatedStorageApiReturnsJson401InsteadOfRedirect() throws Exception {
        mockMvc.perform(get("/api/storages/databases")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isUnauthorized())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.code").value("UNAUTHORIZED"))
                .andExpect(jsonPath("$.message").value("로그인이 필요해요. 다시 로그인한 뒤 시도해 주세요."));
    }

    @Test
    void unauthenticatedStoragePageStillRedirectsToLogin() throws Exception {
        mockMvc.perform(get("/storages/databases"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrlPattern("**/"));
    }

    @Test
    @WithMockUser(username = "tester")
    void mutatingStorageRequestRequiresCsrfAndReturnsJson403() throws Exception {
        mockMvc.perform(post("/api/storages/databases/test")
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo\",\"encryptionKey\":\"secret-secret-secret\",\"vendor\":\"POSTGRESQL\",\"authMethod\":\"PASSWORD\",\"url\":\"localhost:5432/demo\",\"username\":\"tester\",\"password\":\"pw\"}"))
                .andExpect(status().isForbidden())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.code").value("FORBIDDEN"));
    }

    @Test
    @WithMockUser(username = "tester")
    void mutatingStorageRequestSucceedsWithCsrfHeader() throws Exception {
        given(connectionTestService.test(any())).willReturn(DatabaseConnectionTestResponse.builder()
                .success(true)
                .message("ok")
                .build());

        mockMvc.perform(post("/api/storages/databases/test")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo\",\"encryptionKey\":\"secret-secret-secret\",\"vendor\":\"POSTGRESQL\",\"authMethod\":\"PASSWORD\",\"url\":\"localhost:5432/demo\",\"username\":\"tester\",\"password\":\"pw\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("ok"));
    }

    @Test
    @WithMockUser(username = "tester")
    void revalidateRequiresCsrfAndReturnsJson403() throws Exception {
        mockMvc.perform(post("/api/storages/databases/1/revalidate")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isForbidden())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.code").value("FORBIDDEN"));
    }

    @Test
    @WithMockUser(username = "tester")
    void revalidateSucceedsWithCsrf() throws Exception {
        given(managementService.revalidate(1L)).willReturn(DatabaseConnectionTestResponse.builder()
                .success(true)
                .message("revalidated")
                .build());

        mockMvc.perform(post("/api/storages/databases/1/revalidate")
                        .with(csrf())
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.message").value("revalidated"));
    }

    @Test
    @WithMockUser(username = "tester")
    void updateRequiresCsrfAndReturnsJson403() throws Exception {
        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put("/api/storages/databases/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo\",\"vendor\":\"POSTGRESQL\",\"authMethod\":\"PASSWORD\",\"jdbcUrl\":\"localhost:5432/demo\"}"))
                .andExpect(status().isForbidden())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.code").value("FORBIDDEN"));
    }

    @Test
    @WithMockUser(username = "tester")
    void updateSucceedsWithCsrf() throws Exception {
        given(managementService.update(anyLong(), any())).willReturn(DatabaseStorageDetailResponse.builder()
                .id(1L)
                .name("demo-updated")
                .description("desc")
                .storageType("DATABASE")
                .vendor(DatabaseVendor.POSTGRESQL)
                .authMethod(DatabaseAuthMethod.PASSWORD)
                .jdbcUrl("localhost:5432/demo")
                .port(5432)
                .username("tester")
                .schemaName("public")
                .databaseName("demo")
                .connectionStatus("PENDING")
                .createdAt("2026-03-23T15:00:00")
                .updatedAt("2026-03-23T16:00:00")
                .build());

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put("/api/storages/databases/1")
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .accept(MediaType.APPLICATION_JSON)
                        .content("{\"name\":\"demo-updated\",\"vendor\":\"POSTGRESQL\",\"authMethod\":\"PASSWORD\",\"jdbcUrl\":\"localhost:5432/demo\",\"port\":5432,\"databaseName\":\"demo\",\"username\":\"tester\",\"password\":\"pw\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(1))
                .andExpect(jsonPath("$.name").value("demo-updated"))
                .andExpect(jsonPath("$.connectionStatus").value("PENDING"));
    }

    @Test
    @WithMockUser(username = "tester")
    void deleteRequiresCsrfAndReturnsJson403() throws Exception {
        mockMvc.perform(delete("/api/storages/databases/1")
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isForbidden())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.code").value("FORBIDDEN"));
    }

    @Test
    @WithMockUser(username = "tester")
    void deleteSucceedsWithCsrf() throws Exception {
        mockMvc.perform(delete("/api/storages/databases/1")
                        .with(csrf())
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isNoContent());
    }
}
