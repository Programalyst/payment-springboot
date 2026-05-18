package com.demo.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        // This line starts the entire IoC container and manages your beans
        ApplicationContext context = SpringApplication.run(Application.class, args);

    }

}
