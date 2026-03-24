package com.etl.sfdc.navigation.controller;

import com.etl.sfdc.common.SalesforceTokenManager;
import com.etl.sfdc.common.SecurityConfig;
import com.etl.sfdc.common.UserSession;
import com.etl.sfdc.config.model.service.SalesforceOrgService;
import com.etl.sfdc.etl.controller.ETLController;
import com.etl.sfdc.etl.service.ETLService;
import com.etl.sfdc.home.controller.HomeController;
import com.etl.sfdc.storage.service.DatabaseStorageQueryService;
import com.etl.sfdc.user.controller.UserController;
import com.etl.sfdc.user.model.service.UserService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(
        controllers = {
                HomeController.class,
                UserController.class,
                ETLController.class
        },
        properties = "app.db.enabled=true"
)
@Import(SecurityConfig.class)
class NavigationFlowWebMvcTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private UserSession userSession;

    @MockBean
    private SalesforceOrgService salesforceOrgService;

    @MockBean
    private SalesforceTokenManager salesforceTokenManager;

    @MockBean
    private UserService userService;

    @MockBean
    private ETLService etlService;

    @MockBean
    private DatabaseStorageQueryService databaseStorageQueryService;

    @Test
    void homePageRendersLoginFormForAnonymousUsers() throws Exception {
        mockMvc.perform(get("/"))
                .andExpect(status().isOk())
                .andExpect(content().string(containsString("id=\"login-form\"")))
                .andExpect(content().string(containsString("action=\"/user/login\"")))
                .andExpect(content().string(containsString("메인 페이지에서 바로 로그인")));
    }

    @Test
    void legacyLoginPageRedirectsToHomePreservingQueryString() throws Exception {
        mockMvc.perform(get("/user/login").param("error", "login"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/?error=login"));
    }

    @Test
    @WithMockUser(username = "tester")
    void directGetDdlRedirectsToObjectSelectionWithFriendlyMessage() throws Exception {
        mockMvc.perform(get("/etl/ddl").param("orgKey", "demo-org"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/etl/objects?message=ddl_direct_access&orgKey=demo-org"));
    }

    @Test
    @WithMockUser(username = "tester")
    void directRouteDetailAccessWithoutContextRedirectsToDashboardWithFriendlyMessage() throws Exception {
        mockMvc.perform(get("/etl/routes/detail").param("orgKey", "demo-org"))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/etl/dashboard?message=route_detail_direct_access&orgKey=demo-org"));
    }
}
