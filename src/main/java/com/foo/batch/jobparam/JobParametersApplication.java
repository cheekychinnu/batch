package com.foo.batch.jobparam;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@SpringBootApplication
public class JobParametersApplication  {

    // EDIT run configuration pass "message=hello" in the program arguments.
    public static void main(String[] args) {

        for(String a: args) {
            System.out.println(a);
        }
        SpringApplication.run(JobParametersApplication.class, args);
    }
}
