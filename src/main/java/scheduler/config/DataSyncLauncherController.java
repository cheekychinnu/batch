package scheduler.config;


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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.WebRequest;
import scheduler.dao.SchedulerDao;

import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/sync")
public class DataSyncLauncherController {

    private final static String JOB_NAME = "data-sync";
    @Autowired
    private JobOperator jobOperator;
    @Autowired
    private JobExplorer jobExplorer;
    @Autowired
    private SchedulerDao schedulerDao;
    @Autowired
    private Job job;
    @Autowired
    private DataSyncScheduler dataSyncScheduler;

    @RequestMapping(value = "/launch/", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Long launchChildJob(@RequestParam("name") String name, @RequestParam("childJob") String jobName)
            throws JobParametersInvalidException, NoSuchJobException, JobInstanceAlreadyExistsException {
        return this.jobOperator.start(jobName, String.format("name=%s", name));
    }

   /* @RequestMapping(value = "/launchOverride/", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Long launchOverride(@RequestParam("name") String name) throws JobParametersInvalidException,
            NoSuchJobException, JobInstanceAlreadyExistsException {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("name", name)
                .addString("override", "true")
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();
        return this.jobOperator.start(JOB_NAME + "-override", String.format("name=%s,time=%s", name, System.currentTimeMillis()));
    }*/

    @RequestMapping(value = "/checkStatus/{id}", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public BatchStatus checkStatus(@PathVariable("id") Long executionId) {
        JobExecution jobExecution = this.jobExplorer.getJobExecution(executionId);
        return jobExecution.getStatus();
    }

    @RequestMapping(value = "/bootstrap/", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public Long bootstrap(@RequestParam("name") String name) throws InterruptedException, JobParametersInvalidException,
            JobInstanceAlreadyExistsException, NoSuchJobException {
        dataSyncScheduler.pause();
        while (schedulerDao.isLastExecutionAtNonOverrideModeStillRunning(JOB_NAME)) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        Long job = null;
        try {
            job = this.jobOperator.start(JOB_NAME, "name=" + name);
        } finally {
            dataSyncScheduler.unpause();
        }
        return job;
    }

    @RequestMapping(value = "/disable", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public void disable() throws InterruptedException {
        dataSyncScheduler.pause();
    }

    @RequestMapping(value = "/enable", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public void enable() throws InterruptedException {
        dataSyncScheduler.unpause();
    }

    @RequestMapping(value = "/retry/{id}", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public Long retry(@PathVariable("id") Long executionId) throws InterruptedException,
            JobParametersInvalidException, JobRestartException, JobInstanceAlreadyCompleteException,
            NoSuchJobExecutionException, NoSuchJobException {
        return this.jobOperator.restart(executionId);
    }

    @ExceptionHandler(Exception.class)
    public final ResponseEntity<String> handleAllExceptions(Exception ex, WebRequest request) {
        return new ResponseEntity<>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

}
