package com.foo.batch.joborchestrationstop.config;

import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.*;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
public class JobLauncherController {

    @Autowired
    private JobOperator jobOperator;


    @RequestMapping(value = "/", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Long launch(@RequestParam("name") String name) throws JobParametersInvalidException, JobExecutionAlreadyRunningException,
            JobRestartException, JobInstanceAlreadyCompleteException, JobInstanceAlreadyExistsException, NoSuchJobException {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("name", name)
                .toJobParameters();
        return this.jobOperator.start("job", String.format("name=%s", name));
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    @ResponseStatus(HttpStatus.OK)
    public void stop(@PathVariable("id") Long id) throws NoSuchJobExecutionException, JobExecutionNotRunningException {
        this.jobOperator.stop(id);
    }
}
