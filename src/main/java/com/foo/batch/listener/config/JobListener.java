package com.foo.batch.listener.config;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class JobListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println(jobExecution.getJobInstance().getJobName() + " is starting..");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println(jobExecution.getJobInstance().getJobName() + " is done");
    }

}

