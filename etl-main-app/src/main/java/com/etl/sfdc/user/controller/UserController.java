package com.etl.sfdc.user.controller;

import com.etlplatform.common.error.FeatureDisabledException;
import com.etl.sfdc.user.model.dto.UserCreateForm;
import com.etl.sfdc.user.model.service.UserService;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

@Controller
@RequiredArgsConstructor
@RequestMapping("user")
public class UserController {

    private final UserService userService;

    @Value("${app.db.enabled:false}")
    private boolean dbEnabled;

    @GetMapping("/login")
    public String loginPage(Model model) {
        model.addAttribute("dbEnabled", dbEnabled);
        return "login_form";
    }

    @GetMapping("/signup")
    public String signup(Model model) {
        model.addAttribute("dbEnabled", dbEnabled);
        model.addAttribute("UserCreateForm", new UserCreateForm());
        return "signup_form";
    }

    @PostMapping("/signup")
    public String signup(@Valid UserCreateForm userCreateForm, BindingResult bindingResult, Model model) throws JsonProcessingException {
        model.addAttribute("dbEnabled", dbEnabled);

        if (!dbEnabled) {
            throw new FeatureDisabledException("DB 비활성 모드에서는 회원가입을 사용할 수 없습니다.");
        }

        userService.create(userCreateForm.getUsername(),
                userCreateForm.getEmail(), userCreateForm.getPassword1(), userCreateForm.getDescription());

        return "redirect:/";
    }
}
