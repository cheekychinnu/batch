package com.foo.batch.joborchestration;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing // give Job launcher
public class StartingAJobApplication {
    // set spring.batch.job.enabled = false for this demo.
    // you do not want the container to run all the jobs when it starts up.

    // hit http://localhost:8080/?name=bar in your browser

    public static void main(String[] args) {
        SpringApplication.run(StartingAJobApplication.class, args);
    }
}
