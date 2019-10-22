package com.foo.batch.complicatedflows;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class ComplicatedNestedJobFlows {
    public static void main(String[] args) {
        SpringApplication.run(ComplicatedNestedJobFlows.class,args);
    }
}
