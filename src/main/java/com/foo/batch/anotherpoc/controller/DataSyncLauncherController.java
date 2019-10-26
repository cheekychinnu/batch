package com.foo.batch.anotherpoc.controller;


import com.foo.batch.anotherpoc.service.DataSyncService;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.WebRequest;

@RestController
@RequestMapping("/data-sync")
public class DataSyncLauncherController {

    @Autowired
    private DataSyncService dataSyncService;

    @RequestMapping(value = "/launch/", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Long launch(@RequestParam("name") String name, @RequestParam("jobName") String jobName)
            throws JobParametersInvalidException, NoSuchJobException, JobInstanceAlreadyExistsException {
        return dataSyncService.runDataSyncOverrideMode(name, jobName);
    }

    @RequestMapping(value = "/checkStatus/{id}", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public BatchStatus checkStatus(@PathVariable("id") Long executionId) {
        return dataSyncService.getJobExecutionStatus(executionId);
    }

    @RequestMapping(value = "/bootstrap/", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public Long bootstrap(@RequestParam("name") String name) throws InterruptedException, JobParametersInvalidException,
            JobInstanceAlreadyExistsException, NoSuchJobException {
        return dataSyncService.bootstrapDataSyncScheduler(name);
    }

    @RequestMapping(value = "/pause", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public void pause() throws InterruptedException {
        dataSyncService.pauseDataSyncScheduler();
    }

    @RequestMapping(value = "/unpause", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public void unpause() throws InterruptedException {
        dataSyncService.unpauseDataSyncScheduler();
    }

    @RequestMapping(value = "/retry/{id}", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public Long retry(@PathVariable("id") Long executionId) throws InterruptedException,
            JobParametersInvalidException, JobRestartException, JobInstanceAlreadyCompleteException,
            NoSuchJobExecutionException, NoSuchJobException {
        return dataSyncService.retry(executionId);
    }

    @ExceptionHandler(Exception.class)
    public final ResponseEntity<String> handleAllExceptions(Exception ex, WebRequest request) {
        return new ResponseEntity<>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }

}
