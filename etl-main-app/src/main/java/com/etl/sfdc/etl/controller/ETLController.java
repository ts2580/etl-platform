package com.etl.sfdc.etl.controller;

import com.etl.sfdc.common.UserSession;
import com.etl.sfdc.etl.dto.ObjectDefinition;
import com.etl.sfdc.etl.service.ETLService;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.util.List;

@Controller
@RequiredArgsConstructor
@RequestMapping("etl")
public class ETLController {

    private final UserSession userSession;

    private final ETLService etlService;

    @GetMapping("/objects")
    public String getObjects(Model model, HttpSession session, HttpServletResponse response) throws Exception {

        String accessToken = (String) session.getAttribute("accessToken");

        if (accessToken == null) {
            response.sendRedirect("/login");
            return null;
        }

        List<ObjectDefinition> objectDefinitions = etlService.getObjects(accessToken);

        if(userSession.getUserAccount() != null){
            model.addAttribute(userSession.getUserAccount().getMember());
            model.addAttribute("objectDefinitions", objectDefinitions);
        }

        return "object_select_form";

    }

    @PostMapping("/ddl")
    public String setObjects(@RequestParam("selectedObject") String selectedObject, Model model, HttpSession session) throws Exception {

        String accessToken = (String) session.getAttribute("accessToken");
        String refreshToken = (String) session.getAttribute("refreshToken");

        if(userSession.getUserAccount() != null){
            model.addAttribute(userSession.getUserAccount().getMember());
        }

        etlService.setObjects(selectedObject, accessToken, refreshToken);

        return "home_form";
    }
}