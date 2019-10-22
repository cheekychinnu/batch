package com.foo.batch.complicatedflows.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.JobStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Configuration
public class AppConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobLauncher jobLauncher;

    @Bean
    public Job referenceJob() {
        return jobBuilderFactory.get("reference-job")
                .start(stepBuilderFactory.get("reference-etl")
                        .tasklet((contribution, chunkContext) -> {
                            Random random = new Random();
                            int duration = random.nextInt(6000);
                            System.out.println(Thread.currentThread().getName() + " Executing reference reader. Going to sleep for " + duration);
                            TimeUnit.MILLISECONDS.sleep(duration);
                            System.out.println(Thread.currentThread().getName() + " Executing reference reader. Finished");
                            return RepeatStatus.FINISHED;
                        }).build()).build();
    }

    @Bean
    public Job tradeJob() {
        return jobBuilderFactory.get("trade-job")
                .start(stepBuilderFactory.get("trade-etl")
                        .tasklet((contribution, chunkContext) -> {
                            Random random = new Random();
                            int duration = random.nextInt(6000);
                            System.out.println(Thread.currentThread().getName() + " Executing trade reader. Going to sleep for " + duration);
                            TimeUnit.MILLISECONDS.sleep(duration);
                            System.out.println(Thread.currentThread().getName() + " Executing trade reader. Finished");
                            return RepeatStatus.FINISHED;
                        }).build()).build();
    }

    @Bean
    public Job pricingJob() {
        return jobBuilderFactory.get("pricing-job")
                .start(stepBuilderFactory.get("pricing-etl")
                        .tasklet((contribution, chunkContext) -> {
                            Random random = new Random();
                            int duration = random.nextInt(6000);
                            System.out.println(Thread.currentThread().getName() + " Executing pricing reader");
                            TimeUnit.MILLISECONDS.sleep(duration);
                            System.out.println(Thread.currentThread().getName() + " Executing pricing reader. Finished");
                            return RepeatStatus.FINISHED;
                        }).build()).build();
    }

    @Bean
    public Step finalStep() {
        return stepBuilderFactory.get("final-send")
                .tasklet((contribution, chunkContext) -> {
                    System.out.println(Thread.currentThread().getName() + " Final");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Job dataSync(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        Step pricingJobStep = new JobStepBuilder(new StepBuilder("pricingJobStep"))
                .job(pricingJob())
                .launcher(jobLauncher)
                .repository(jobRepository)
                .transactionManager(transactionManager)
                .build();
        Flow pricingFlow = new FlowBuilder<Flow>("pricing-flow").start(
                pricingJobStep
        ).build();

        Step referenceJobStep = new JobStepBuilder(new StepBuilder("referenceJobStep"))
                .job(referenceJob())
                .launcher(jobLauncher)
                .repository(jobRepository)
                .transactionManager(transactionManager)
                .build();

        Flow referenceFlow = new FlowBuilder<Flow>("reference-flow").start(
                referenceJobStep
        ).build();
        Step tradeJobStep = new JobStepBuilder(new StepBuilder("tradeJobStep"))
                .job(tradeJob())
                .launcher(jobLauncher)
                .repository(jobRepository)
                .transactionManager(transactionManager)
                .build();
        Flow tradeFlow = new FlowBuilder<Flow>("trade-flow").start(
                tradeJobStep
        ).build();
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();

        Flow etlFlow = new FlowBuilder<Flow>("etl-flow")
                .start(pricingFlow)
                .split(simpleAsyncTaskExecutor)
                .add(referenceFlow)
                .split(simpleAsyncTaskExecutor)
                .add(tradeFlow)
                .end();

        return jobBuilderFactory.get("data-sync")
                .start(etlFlow)
                .next(finalStep())
                .end().build();
    }
}
