package com.etl.sfdc.user.controller;

import com.etl.sfdc.user.model.dto.UserCreateForm;
import com.etl.sfdc.user.model.service.UserService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;

@Controller
@RequiredArgsConstructor
@RequestMapping("user")
public class UserController {

    private final UserService userService;

    @GetMapping("/login")
    public String loginPage() {
        return "login_form";
    }

    @GetMapping("/signup")
    public String signup(Model model) {

        // 폼에 바인딩할 빈 객체를 모델에 추가
        model.addAttribute("UserCreateForm", new UserCreateForm());

        return "signup_form";
    }

    @PostMapping("/signup")
    public String signup(@Valid UserCreateForm userCreateForm, BindingResult bindingResult) throws JsonProcessingException {

        // signup_form에 객체 바인딩 하기

        /*if (bindingResult.hasErrors()) {
            return "signup_form";
        }

        if (!userCreateForm.getPassword1().equals(userCreateForm.getPassword2())) {
            bindingResult.rejectValue("password2", "passwordInCorrect", "2개의 패스워드가 일치하지 않습니다.");
            return "signup_form";
        }*/

        userService.create(userCreateForm.getUsername(),
                userCreateForm.getEmail(), userCreateForm.getPassword1(), userCreateForm.getDescription());

        return "redirect:/";
    }

}
