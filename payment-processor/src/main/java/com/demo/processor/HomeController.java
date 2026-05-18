package com.demo.processor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class HomeController {

    @Value("${server.port}")
    private String PORT;

    @RequestMapping("/")
    public String home() {
        System.out.printf("PORT %s", PORT);
        return "index.html";
    }
}
