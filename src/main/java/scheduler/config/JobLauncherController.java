package scheduler.config;


import org.springframework.batch.core.*;
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
public class JobLauncherController {

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private SchedulerDao schedulerDao;

    @Autowired
    private Job job;

    @Autowired
    private Scheduler scheduler;

    // TODO: yet to be implemented.
    @RequestMapping(value = "/getExecutionId/", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Long getExecutionId(@RequestParam("name") String name) throws JobParametersInvalidException, NoSuchJobException {
        Long jobExecutionId = schedulerDao.getLastExecutionMatchingParameter("job", name);
        return jobExecutionId;
    }

    @RequestMapping(value = "/launchOverride/", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Long launchOverride(@RequestParam("name") String name) throws JobParametersInvalidException,
            NoSuchJobException, JobInstanceAlreadyExistsException {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("name", name)
                .addString("override", "true")
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();
        return this.jobOperator.start("job-override", String.format("name=%s,time=%s", name, System.currentTimeMillis()));
    }

    @RequestMapping(value = "/checkStatus/{id}", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public BatchStatus checkStatus(@PathVariable("id") Long executionId) {
        JobExecution jobExecution = this.jobExplorer.getJobExecution(executionId);
        return jobExecution.getStatus();
    }

    @RequestMapping(value = "/bootstrap/", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public Long bootstrap(@RequestParam("name") String name) throws InterruptedException, JobParametersInvalidException, JobInstanceAlreadyExistsException, NoSuchJobException {
        scheduler.pause();
        while(schedulerDao.isLastExecutionAtNonOverrideModeStillRunning("job")) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        Long job = null;
        try {
            job = this.jobOperator.start("job", "name=" + name);
        } finally {
            scheduler.unpause();
        }
        return job;
    }

    @RequestMapping(value = "/disable", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public void disable() throws InterruptedException {
        scheduler.pause();
    }

    @RequestMapping(value = "/enable", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public void enable() throws InterruptedException {
        scheduler.unpause();
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
