package com.foo.batch.skip;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class SkipApplication {
    // Pass program argument - skip=processor or skip=writer to see how it behaves.
    public static void main(String[] args) {
        SpringApplication.run(SkipApplication.class, args);
    }
}
