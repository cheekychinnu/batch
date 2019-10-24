package scheduler.config;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.JobStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Configuration
@Profile("scheduler")
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

    @Bean
    @StepScope
    public Tasklet tasklet(@Value("#{jobParameters['name']}") String name) {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                int duration = random.nextInt(6000) + 2000;
                System.out.println("Job executing for " + name + " Going to sleep for " + duration);
                TimeUnit.MILLISECONDS.sleep(duration);
                System.out.println("Job ran for " + name);
                if (duration % 2 == 0) {
                    // when the execution id is retried, we are letting it complete successfully.
                    if (!chunkContext.getStepContext().getStepExecutionContext().containsKey("isRestart")) {
                        chunkContext.getStepContext().getStepExecution().getExecutionContext().put("isRestart", true);
                        throw new RuntimeException("Randomly failing " + name);
                    }
                }
                return RepeatStatus.FINISHED;
            }
        };
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

    private SimpleJobBuilder jobBuilder(String name) {
        return jobBuilderFactory.get(name)
//                .incrementer(new RunIdIncrementer()) // need only when you invoke startNextInstance()
                .start(stepBuilderFactory.get("step1")
                        .tasklet(tasklet(null))
                        .build());
    }

    @Bean
    public Job job() {
        return jobBuilder("job").build();
    }

    @Bean
    public Job overrideJob() {
        return jobBuilder("job-override").build();
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Bean
    public Job referenceJob() {
        return jobBuilderFactory.get("reference-job")
                .start(stepBuilderFactory.get("reference-etl")
                        .tasklet((contribution, chunkContext) -> {
                            Random random = new Random();
                            int duration = random.nextInt(6000);
                            System.out.println(Thread.currentThread().getName() + " Executing reference reader for "+
                                    chunkContext.getStepContext().getJobParameters().get("name") +
                                    ". Going to sleep for " + duration);

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
                        }).build())
                .build();
    }

    @Bean
    public Job pricingJob() {
        return jobBuilderFactory.get("pricing-job")
                .start(stepBuilderFactory.get("pricing-etl")
                        .tasklet((contribution, chunkContext) -> {
                            Random random = new Random();
                            int duration = random.nextInt(6000);
                            System.out.println(Thread.currentThread().getName() + " Executing pricing reader for "+
                                    chunkContext.getStepContext().getJobParameters().get("name") + " going to sleep for "+duration);
                            TimeUnit.MILLISECONDS.sleep(duration);
                            System.out.println(Thread.currentThread().getName() + " Executing pricing reader. Finished");
                            return RepeatStatus.FINISHED;
                        }).build())
                .build();
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
    public Job dataSync(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        Step pricingJobStep = new JobStepBuilder(new StepBuilder("pricingJobStep"))
                .job(pricingJob())
//                .launcher(jobLauncher)
                .repository(jobRepository)
                .transactionManager(transactionManager)
                .build();
        Flow pricingFlow = new FlowBuilder<Flow>("pricing-flow").start(
                pricingJobStep
        ).build();

        Step referenceJobStep = new JobStepBuilder(new StepBuilder("referenceJobStep"))
                .job(referenceJob())
//                .launcher(jobLauncher)
                .repository(jobRepository)
                .transactionManager(transactionManager)
                .build();

        Flow referenceFlow = new FlowBuilder<Flow>("reference-flow").start(
                referenceJobStep
        ).build();
        Step tradeJobStep = new JobStepBuilder(new StepBuilder("tradeJobStep"))
                .job(tradeJob())
//                .launcher(jobLauncher)
                .repository(jobRepository)
                .transactionManager(transactionManager)
                .build();
        Flow tradeFlow = new FlowBuilder<Flow>("trade-flow").start(
                tradeJobStep
        ).build();
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor("ETL-EXEC");

        Flow etlFlow = new FlowBuilder<Flow>("etl-flow")
                .split(simpleAsyncTaskExecutor)
                .add(pricingFlow,referenceFlow,tradeFlow)
                .build();

//        FlowStep etlFlowStep = new FlowStep(etlFlow);
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