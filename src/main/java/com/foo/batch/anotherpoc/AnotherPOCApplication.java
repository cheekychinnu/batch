package com.foo.batch.anotherpoc;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

// Run this with -Dspring.profiles.active=schedulerpoc
@SpringBootApplication
@EnableBatchProcessing
@EnableScheduling
public class AnotherPOCApplication {
    public static void main(String[] args) {
        SpringApplication
                .run(AnotherPOCApplication.class, args);
    }
}
