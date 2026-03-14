package com.etl.sfdc.home.controller;

import com.etl.sfdc.common.UserSession;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
@RequiredArgsConstructor
public class HomeController {

    private final UserSession userSession;

    @Value("${app.db.enabled:false}")
    private boolean dbEnabled;

    @GetMapping("/")
    public String loginPage(Model model) {
        model.addAttribute("dbEnabled", dbEnabled);

        if (userSession.getUserAccount() != null) {
            model.addAttribute(userSession.getUserAccount().getMember());
        }

        return "home_form";
    }
}
