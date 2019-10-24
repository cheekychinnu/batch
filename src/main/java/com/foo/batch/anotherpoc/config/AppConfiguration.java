package com.foo.batch.anotherpoc.config;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Configuration
@Profile("schedulerpoc")
public class AppConfiguration extends DefaultBatchConfigurer implements ApplicationContextAware {

    Random random = new Random();
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private JobLauncher jobLauncher;
    @Autowired
    private JobRepository jobRepository;
    @Autowired
    private JobRegistry jobRegistry;
    @Autowired
    private JobExplorer jobExplorer;

    private ApplicationContext applicationContext;

    @Bean // to register the job into the registry
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor() throws Exception {
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(this.jobRegistry);
        jobRegistryBeanPostProcessor.setBeanFactory(this.applicationContext.getAutowireCapableBeanFactory());
        jobRegistryBeanPostProcessor.afterPropertiesSet();
        return jobRegistryBeanPostProcessor;
    }

    @Bean // this job operator is needed in order to handle restarts and all
    public JobOperator jobOperator() throws Exception {
        SimpleJobOperator simpleJobOperator = new SimpleJobOperator();

        simpleJobOperator.setJobLauncher(this.jobLauncher);
        simpleJobOperator.setJobParametersConverter(new DefaultJobParametersConverter());
        simpleJobOperator.setJobRepository(this.jobRepository);
        simpleJobOperator.setJobExplorer(this.jobExplorer);
        simpleJobOperator.setJobRegistry(this.jobRegistry);

        simpleJobOperator.afterPropertiesSet();
        return simpleJobOperator;
    }

    @Override
    public JobLauncher getJobLauncher() {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(this.jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor("EXEC-JL-"));
        try {
            jobLauncher.afterPropertiesSet();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jobLauncher;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Bean
    public Step referenceETStep() {
        return stepBuilderFactory.get("reference-et-step")
                .tasklet((contribution, chunkContext) -> {
                    Random random = new Random();
                    int duration = random.nextInt(6000);
                    System.out.println(Thread.currentThread().getName() + " Executing reference reader for "+
                            chunkContext.getStepContext().getJobParameters().get("name") +
                            ". Going to sleep for " + duration);

                    TimeUnit.MILLISECONDS.sleep(duration);
                    System.out.println(Thread.currentThread().getName() + " Executing reference reader. Finished");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Flow referenceETFlow() {
        return new FlowBuilder<Flow>("referenceETFlow").start(referenceETStep()).end();
    }

    @Bean
    public Step tradeETStep() {
        return stepBuilderFactory.get("trade-et-step")
                .tasklet((contribution, chunkContext) -> {
                    boolean fail = true;
                    if (chunkContext.getStepContext().getStepExecutionContext().containsKey("restart")) {
                        fail = false;
                    } else {
                        chunkContext.getStepContext().getStepExecution().getExecutionContext()
                                .put("restart", true);
                    }

                    if (fail) {
                        throw new RuntimeException("Intentional exception");
                    }
                    Random random = new Random();
                    int duration = random.nextInt(6000);
                    System.out.println(Thread.currentThread().getName() + " Executing trade reader for"
                            + chunkContext.getStepContext().getJobParameters().get("name") +
                            ". Going to sleep for " + duration);
                    TimeUnit.MILLISECONDS.sleep(duration);
                    System.out.println(Thread.currentThread().getName() + " Executing trade reader. Finished");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Flow tradeETFlow() {
        return new FlowBuilder<Flow>("tradeETFlow").start(tradeETStep()).end();
    }

    @Bean
    public Step pricingETStep() {
        return stepBuilderFactory.get("pricing-et-step")
                .tasklet((contribution, chunkContext) -> {
                    Random random = new Random();
                    int duration = random.nextInt(6000);
                    System.out.println(Thread.currentThread().getName() + " Executing pricing reader for "+
                            chunkContext.getStepContext().getJobParameters().get("name") + " going to sleep for "+duration);
                    TimeUnit.MILLISECONDS.sleep(duration);
                    System.out.println(Thread.currentThread().getName() + " Executing pricing reader. Finished");
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Flow pricingETFlow() {
        return new FlowBuilder<Flow>("pricingETFlow").start(pricingETStep()).end();
    }

    @Bean
    public Step finalStep() {
        return stepBuilderFactory.get("final-send")
                .tasklet((contribution, chunkContext) -> {
                    System.out.println(Thread.currentThread().getName() + " Final for job parameter : " +
                            chunkContext.getStepContext().getJobParameters().get("name"));
                    return RepeatStatus.FINISHED;
                })
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        Collection<StepExecution> stepExecutions = stepExecution.getJobExecution().getStepExecutions();
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        boolean subJobsFailed = stepExecution.getJobExecution()
                                .getStepExecutions().stream().anyMatch(
                                        s -> s.getExitStatus().getExitCode().equals(ExitStatus.FAILED.getExitCode())
                                );
                        if(subJobsFailed) {
                            stepExecution.setStatus(BatchStatus.FAILED);
                            stepExecution.setExitStatus(ExitStatus.FAILED);
                            return ExitStatus.FAILED;
                        }
                        return stepExecution.getExitStatus();
                    }
                })
                .build();
    }

    @Bean
    public Job dataSync() {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor("ETL-EXEC");

        Flow etlFlow = new FlowBuilder<Flow>("et-flow-split")
                .split(simpleAsyncTaskExecutor)
                .add(pricingETFlow(),referenceETFlow(),tradeETFlow())
                .end();
        return jobBuilderFactory.get("data-sync")
                .start(etlFlow)
                .on("COMPLETED")
                .to(finalStep())
                .from(etlFlow)
                .on("FAILED")
                .to(finalStep())
                .end().build();
    }

}