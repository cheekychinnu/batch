package com.foo.batch.anotherpoc.service;

import com.foo.batch.anotherpoc.config.DataSyncScheduler;
import com.foo.batch.anotherpoc.dao.EtlJobDao;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class DataSyncService {

    private final static String JOB_NAME = "data-sync";
    private final static String OVERRIDE_TAG = "-override";

    @Autowired
    private EtlJobDao etlJobDao;
    @Autowired
    private JobOperator jobOperator;
    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private DataSyncScheduler dataSyncScheduler;

    public Long runDataSyncOverrideMode(String jobParameter, String jobName)
            throws JobParametersInvalidException, JobInstanceAlreadyExistsException, NoSuchJobException {

        if(!etlJobDao.isActive(jobName+OVERRIDE_TAG)) {
            throw new IllegalArgumentException(jobName + " is disabled");
        }

        return this.jobOperator.start(jobName+OVERRIDE_TAG,
                String.format("name=%s,time=%s", jobParameter, System.currentTimeMillis()));
    }

    public BatchStatus getJobExecutionStatus(Long executionId) {
        JobExecution jobExecution = this.jobExplorer.getJobExecution(executionId);
        return jobExecution.getStatus();
    }

    public Long bootstrapDataSyncScheduler(String jobParameters) throws InterruptedException, JobParametersInvalidException, JobInstanceAlreadyExistsException, NoSuchJobException {
        dataSyncScheduler.pause();
        while (dataSyncScheduler.isLastExecutionAtNonOverrideModeStillRunning(JOB_NAME)) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        Long job = null;
        try {
            job = this.jobOperator.start(JOB_NAME, "name=" + jobParameters);
        } finally {
            dataSyncScheduler.unpause();
        }
        return job;
    }

    public void unpauseDataSyncScheduler() {
        dataSyncScheduler.unpause();
    }

    public void pauseDataSyncScheduler() {
        dataSyncScheduler.pause();
    }

    public Long retry(Long executionId) throws JobParametersInvalidException, JobRestartException, JobInstanceAlreadyCompleteException, NoSuchJobExecutionException, NoSuchJobException {
        return this.jobOperator.restart(executionId);
    }
}
