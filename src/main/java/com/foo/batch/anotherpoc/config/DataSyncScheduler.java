package com.foo.batch.anotherpoc.config;


import com.foo.batch.anotherpoc.dao.SchedulerDao;
import com.foo.batch.anotherpoc.mapper.EtlJobConfigurationMapper;
import org.springframework.batch.core.*;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.*;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
@DependsOn("dataSync")
public class DataSyncScheduler {

    private static final String JOB_NAME = "data-sync";

    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private EtlJobConfigurationMapper etlJobConfigurationMapper;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private SchedulerDao schedulerDao;

    @PostConstruct
    public void cleanUpAbandonedJobs() throws JobParametersInvalidException,
            JobRestartException, JobInstanceAlreadyCompleteException, NoSuchJobExecutionException, NoSuchJobException {

        System.out.println("postconstruct invoked");
        // cleanup previous executions if any
        Set<JobExecution> jobExecutions = jobExplorer.findRunningJobExecutions("data-sync");
        System.out.println("Found running executions "+jobExecutions.size());
        for (JobExecution jobExecution : jobExecutions) {
            Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
            for (StepExecution stepExecution : stepExecutions) {
                BatchStatus status = stepExecution.getStatus();
                if (status.isRunning() || status == BatchStatus.STOPPING) {
                    stepExecution.setStatus(BatchStatus.STOPPED);
                    stepExecution.setEndTime(new Date());
                    jobRepository.update(stepExecution);
                }
            }

            jobExecution.setStatus(BatchStatus.STOPPED);
            jobExecution.setEndTime(new Date());
            jobRepository.update(jobExecution);

            Long jobExecutionId = jobExecution.getId();

            this.jobOperator.restart(jobExecutionId);
        }
    }

    @Bean
    public int getFixedDelayForDataSync() {
        return schedulerDao.getCronValue();
    }

    //    https://stackoverflow.com/questions/24033208/how-to-prevent-overlapping-schedules-in-spring
    @Scheduled(fixedDelayString = "#{@getFixedDelayForDataSync}")
    public void runJob() throws JobInstanceAlreadyCompleteException, JobExecutionAlreadyRunningException,
            JobParametersInvalidException, JobRestartException, JobParametersNotFoundException, NoSuchJobException,
            JobInstanceAlreadyExistsException, InterruptedException {

        System.out.println("scheduler invoked");

        // if the instance was stopped abruptly and you reboot again, this will indefinitely wait
        // if the previous instance just started a job execution. so you might have to do a cleanup during bootstrap
        // to stop all the executions : I have added one cleanup logic here.
        // Hence @DependsOn the job itself. because you cannot restart the job before the job bean is initialized.
        while (schedulerDao.isLastExecutionAtNonOverrideModeStillRunning(JOB_NAME)) {
            System.out.println("Previous execution for job is still running.. waiting.");
            TimeUnit.MILLISECONDS.sleep(1000);
        }

        if(!this.etlJobConfigurationMapper.isActive(JOB_NAME)) {
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
        this.etlJobConfigurationMapper.setIsActive(JOB_NAME, false);
    }

    public void unpause() {
        this.etlJobConfigurationMapper.setIsActive(JOB_NAME, true);
    }
}
