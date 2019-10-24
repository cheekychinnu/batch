package com.foo.batch.anotherpoc.dao;

import org.springframework.batch.core.JobParameters;

public interface SchedulerDao {
    JobParameters getJobParameterFromTheLastExecutionAtNonOverrideModeRegardlessOfStatus(String jobName);

    boolean isLastExecutionAtNonOverrideModeStillRunning(String job);

    Long getLastExecutionMatchingParameter(String job, String parameter);

    int getCronValue();
}
