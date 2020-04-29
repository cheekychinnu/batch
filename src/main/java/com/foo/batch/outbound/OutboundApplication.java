package com.foo.batch.outbound;

import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobParametersNotFoundException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
@EnableBatchProcessing
public class OutboundApplication {

    // -Dspring.profiles.active=outbound
    public static void main(String[] args) throws JobParametersInvalidException, JobInstanceAlreadyExistsException, NoSuchJobException, JobExecutionAlreadyRunningException, JobRestartException, JobParametersNotFoundException, JobInstanceAlreadyCompleteException {

        ConfigurableApplicationContext applicationContext = SpringApplication.run(OutboundApplication.class, args);
        JobOperator jobOperator = applicationContext.getBean(JobOperator.class);

        Long executionId = jobOperator.startNextInstance("gear-accounting-pnl-sync");
        System.out.println("Triggered "+executionId);
    }
}