package com.etl.sfdc.home.controller;

import com.etl.sfdc.common.UserSession;
import com.etl.sfdc.config.controller.SalesforceOrgViewHelper;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
@RequiredArgsConstructor
public class HomeController {

    private final UserSession userSession;
    private final SalesforceOrgViewHelper salesforceOrgViewHelper;

    @GetMapping("/")
    public String loginPage(Model model, HttpSession session) {
        salesforceOrgViewHelper.populateHomeModel(model, session);

        if (userSession.getUserAccount() == null) {
            return "login_form";
        }

        return "home_form";
    }
}
