package com.etl.sfdc.user.controller;

import com.etl.sfdc.user.model.dto.UserCreateForm;
import com.etl.sfdc.user.model.service.UserService;
import com.etlplatform.common.error.AppException;
import com.etlplatform.common.error.FeatureDisabledException;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Map;

@Controller
@RequiredArgsConstructor
@RequestMapping("user")
@Slf4j
public class UserController {

    private final UserService userService;

    @Value("${app.db.enabled:false}")
    private boolean dbEnabled;

    @GetMapping("/login")
    public String loginPage(@RequestParam Map<String, String> params) {
        UriComponentsBuilder redirect = UriComponentsBuilder.fromPath("/");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            redirect.queryParam(entry.getKey(), entry.getValue());
        }
        return "redirect:" + redirect.toUriString();
    }

    @GetMapping("/signup")
    public String signup(Model model) {
        model.addAttribute("dbEnabled", dbEnabled);
        model.addAttribute("UserCreateForm", new UserCreateForm());
        return "signup_form";
    }

    @PostMapping("/signup")
    public String signup(@Valid UserCreateForm userCreateForm, BindingResult bindingResult, Model model) {
        model.addAttribute("dbEnabled", dbEnabled);
        model.addAttribute("UserCreateForm", userCreateForm);

        if (!dbEnabled) {
            throw new FeatureDisabledException("DB 비활성 모드에서는 회원가입을 사용할 수 없습니다.");
        }

        if (!bindingResult.hasFieldErrors("password1")
                && !bindingResult.hasFieldErrors("password2")
                && !userCreateForm.getPassword1().equals(userCreateForm.getPassword2())) {
            bindingResult.rejectValue("password2", "password.mismatch", "비밀번호 확인이 일치하지 않습니다.");
        }

        if (bindingResult.hasErrors()) {
            return "signup_form";
        }

        try {
            userService.create(userCreateForm.getName(), userCreateForm.getId(),
                    userCreateForm.getEmail(), userCreateForm.getPassword1(), userCreateForm.getDescription());
            return "redirect:/?signup=success";
        } catch (AppException e) {
            log.warn("Signup failed: id={}, email={}, reason={}", userCreateForm.getId(), userCreateForm.getEmail(), e.getMessage());
            model.addAttribute("signupError", e.getMessage());
            return "signup_form";
        } catch (Exception e) {
            log.error("Unexpected signup error. id={}, email={}", userCreateForm.getId(), userCreateForm.getEmail(), e);
            model.addAttribute("signupError", "회원가입 처리 중 오류가 발생했습니다. 로그를 확인해 주세요.");
            return "signup_form";
        }
    }
}
