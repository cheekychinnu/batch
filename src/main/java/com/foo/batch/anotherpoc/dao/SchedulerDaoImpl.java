package com.foo.batch.anotherpoc.dao;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class SchedulerDaoImpl implements SchedulerDao {

    @Autowired
    private JobExplorer jobExplorer;

    private JobInstance getTheLastJobInstance(String job) {
        // this returns the job instances in decreasing order of created time. this way, we are asking for the last instance triggered.
        // so what happens for retries?
        List<JobInstance> previousInstances = this.jobExplorer.findJobInstancesByJobName(job, 0, 1);
        if(previousInstances.isEmpty()) {
            return null;
        }
        JobInstance jobInstance = previousInstances.get(0);
        return jobInstance;
    }

    @Override
    public JobParameters getJobParameterFromTheLastExecutionAtNonOverrideModeRegardlessOfStatus(String jobName) {
        JobInstance jobInstance = getTheLastJobInstance(jobName);
        if(jobInstance == null) {
            return null;
        }
        List<JobExecution> jobExecutions = this.jobExplorer.getJobExecutions(jobInstance);
        JobParameters jobParameters = jobExecutions.stream().map(e -> e.getJobParameters()).findFirst().get();
        return jobParameters;
    }

    @Override
    public int getCronValue() {
        return 100000;
    }

    @Override
    public boolean isLastExecutionAtNonOverrideModeStillRunning(String jobName) {
        JobInstance jobInstance = getTheLastJobInstance(jobName);
        if(jobInstance == null) {
            return false;
        }
        List<JobExecution> jobExecutions = this.jobExplorer.getJobExecutions(jobInstance);
        return jobExecutions.stream().anyMatch(e->e.isRunning());
    }

    @Override
    public Long getLastExecutionMatchingParameter(String job, String parameter) {
        return null;
    }
}
