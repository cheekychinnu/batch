package com.foo.batch.retry;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class RetryApplication {
    // Pass program argument - retry=processor or retry=writer to see how it behaves.
    public static void main(String[] args) {
        SpringApplication.run(RetryApplication.class, args);
    }
}
