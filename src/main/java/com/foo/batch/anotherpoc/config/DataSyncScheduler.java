package com.foo.batch.anotherpoc.config;


import com.foo.batch.anotherpoc.dao.SchedulerDao;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobParametersNotFoundException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class DataSyncScheduler {

    private static final String JOB_NAME= "data-sync";
    private volatile boolean pause = false;

    @Autowired
    private JobOperator jobOperator;
    @Autowired
    private JobExplorer jobExplorer;
    @Autowired
    private SchedulerDao schedulerDao;

    @Bean
    public int getFixedDelayForDataSync() {
        return schedulerDao.getCronValue();
    }

    //    https://stackoverflow.com/questions/24033208/how-to-prevent-overlapping-schedules-in-spring
    @Scheduled(fixedDelayString = "#{@getFixedDelayForDataSync}")
    public void runJob() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException,
            JobParametersInvalidException, JobRestartException, JobParametersNotFoundException, NoSuchJobException,
            JobInstanceAlreadyExistsException, InterruptedException {

        while (schedulerDao.isLastExecutionAtNonOverrideModeStillRunning(JOB_NAME)) {
            System.out.println("Previous execution for job is still running.. waiting.");
            TimeUnit.MILLISECONDS.sleep(1000);
        }

        if(pause) {
            return;
        }

        JobParameters jobParameters = schedulerDao.getJobParameterFromTheLastExecutionAtNonOverrideModeRegardlessOfStatus(JOB_NAME);
        int count;
        if (jobParameters == null) {
            System.out.println("First run");
            count = 0;
        } else {
            JobParameter name = jobParameters.getParameters().get("name");
            int previousCount = Integer.parseInt(String.valueOf(name.getValue()));
            System.out.println("Not first run. Previous count " + previousCount);
            count = previousCount + 1;
        }
        this.jobOperator.start(JOB_NAME, "name=" + count);
    }

    public void pause() {
        this.pause = true;
    }

    public void unpause() {
        this.pause = false;
    }
}
