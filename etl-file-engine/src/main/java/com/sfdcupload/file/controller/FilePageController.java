package com.sfdcupload.file.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class FilePageController {

    @GetMapping("/file")
    public String fileIndex() {
        return "forward:/index.html";
    }
}
