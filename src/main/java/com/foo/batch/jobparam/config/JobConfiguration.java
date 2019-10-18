package com.foo.batch.jobparam.config;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step step1() {
        return  stepBuilderFactory.get("step1").tasklet(helloWorldTasklet(null)).build();
    }
    @Bean
    public Job jobParametersJob() {
        return jobBuilderFactory.get("jobParametersJob").start(step1()).build();
    }

    @Bean
    @StepScope // instantiate the object once the step that is using it, is called.
    public Tasklet helloWorldTasklet(@Value("#{jobParameters[message]}") String message) {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println(chunkContext.getStepContext().getJobParameters().get("message"));
                System.out.println(message);
                return RepeatStatus.FINISHED;
            }
        };
    }
}
