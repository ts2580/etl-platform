package com.etl.sfdc.home.controller;

import com.etl.sfdc.common.UserSession;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
@RequiredArgsConstructor
public class HomeController {

    private final UserSession userSession;

    @GetMapping("/")
    public String loginPage(Model model) {

        if(userSession.getUserAccount() != null){
            model.addAttribute(userSession.getUserAccount().getMember());
        }

        return "home_form";
    }

}
