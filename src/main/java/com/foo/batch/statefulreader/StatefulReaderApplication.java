package com.foo.batch.statefulreader;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class StatefulReaderApplication {
    public static void main(String[] args) {
        SpringApplication.run(StatefulReaderApplication.class, args);
    }
}
