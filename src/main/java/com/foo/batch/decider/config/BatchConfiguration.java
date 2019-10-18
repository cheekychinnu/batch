package com.foo.batch.decider.config;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BatchConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step startStep() {
        return stepBuilderFactory.get("startStep").tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("This is start tasklet");
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean
    public Step evenStep() {
        return stepBuilderFactory.get("evenStep")
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("This is even tasklet");
                        return RepeatStatus.FINISHED;
                    }
                }).build();
    }

    @Bean
    public Step oddStep() {
        return stepBuilderFactory.get("oddStep")
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                        System.out.println("This is odd tasklet");
                        return RepeatStatus.FINISHED;
                    }
                }).build();
    }

    @Bean
    public JobExecutionDecider decider() {
        return new OddEvenDecider();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("job")
                .start(startStep())
                .next(decider())
                .from(decider()).on("ODD").to(oddStep())
                .from(decider()).on("EVEN").to(evenStep())
                .from(oddStep()).on("*").to(decider())
                // these are redundant.
//                .from(decider()).on("ODD").to(oddStep())
//                .from(decider()).on("EVEN").to(evenStep())
                .end().build();
    }

    // Final output : start, odd, even
    static class OddEvenDecider implements JobExecutionDecider {
        private int count = 0;

        @Override
        public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
            count++;
            if (count % 2 == 0) {
                return new FlowExecutionStatus("EVEN");
            }
            return new FlowExecutionStatus("ODD");
        }
    }
}
