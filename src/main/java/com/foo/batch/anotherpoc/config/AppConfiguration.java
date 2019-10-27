package com.foo.batch.anotherpoc.config;

import com.foo.batch.anotherpoc.dao.DataSyncJobDao;
import com.foo.batch.anotherpoc.domain.DataSyncJobMetadata;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Configuration

@MapperScan("com.foo.batch.anotherpoc.dao.mapper")
@Profile("schedulerpoc")
public class AppConfiguration extends DefaultBatchConfigurer implements ApplicationContextAware {

    public static final String JOB_PARAMETER_KEY = "name";

    public final static String DATA_SYNC_JOB_NAME = "data-sync";
    public final static String OVERRIDE_MODE_TAG ="-override";
    private final static String DATA_SYNC_OVERRIDE_JOB_NAME = DATA_SYNC_JOB_NAME+OVERRIDE_MODE_TAG;

    private static final String PRICE = "price";
    private static final String REFERENCE = "reference";
    private static final String TRADE = "trade";

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
    private DataSyncJobDao dataSyncJobDao;
    @Autowired
    private JobExplorer jobExplorer;

    private ApplicationContext applicationContext;

    @Bean // to register the job into the registry
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor() throws Exception {
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(this.jobRegistry);
        jobRegistryBeanPostProcessor.setBeanFactory(this.applicationContext.getAutowireCapableBeanFactory());
        jobRegistryBeanPostProcessor.afterPropertiesSet();
        System.out.println("job registry initialized");
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
                    System.out.println(Thread.currentThread().getName() + " Executing reference reader for " +
                            chunkContext.getStepContext().getJobParameters().get(JOB_PARAMETER_KEY) +
                            ". Going to sleep for " + duration);

                    TimeUnit.MILLISECONDS.sleep(duration);
                    chunkContext.getStepContext().getStepExecution().getExecutionContext().putString("referenceET", "done for " + duration);
                    System.out.println(Thread.currentThread().getName() + " Executing reference reader. Finished");
                    return RepeatStatus.FINISHED;
                })
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {

                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        // only after the entire step is done, you add it to the Job execution context.
                        // in ideal batch infra, until reference et is done, finalStep() will not even begin but we are overriding that behavior
                        // so it is upto us to do this.
                        // another way is check for the step execution status in finalStep()
                        stepExecution.getJobExecution().getExecutionContext()
                                .putString("referenceET",
                                        stepExecution.getExecutionContext().getString("referenceET"));
                        return stepExecution.getExitStatus();
                    }
                })
                .build();
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

                    Random random = new Random();
                    int duration = random.nextInt(6000);
                    System.out.println(Thread.currentThread().getName() + " Executing trade reader for"
                            + chunkContext.getStepContext().getJobParameters().get(JOB_PARAMETER_KEY) +
                            ". Going to sleep for " + duration);
                    TimeUnit.MILLISECONDS.sleep(duration);
                    if (fail) {
                        throw new RuntimeException("Intentional exception");
                    }
                    chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext()
                            .putString("tradeET", "done for " + duration);
                    // I am intentionally throwing this exception after I have updated the job context
//                    if (fail) {
//                        throw new RuntimeException("Intentional exception");
//                    }
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
                    System.out.println(Thread.currentThread().getName() + " Executing pricing reader for " +
                            chunkContext.getStepContext().getJobParameters().get(JOB_PARAMETER_KEY) + " going to sleep for " + duration);
                    TimeUnit.MILLISECONDS.sleep(duration);
                    chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext().
                            putString("pricingET", "done for " + duration);
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
                    // job execution context is stored between retries.
                    ExecutionContext jobExecutionContext = chunkContext.getStepContext().getStepExecution()
                            .getJobExecution().getExecutionContext();
                    String pricingET = jobExecutionContext.containsKey("pricingET") ? jobExecutionContext
                            .getString("pricingET") : null;
                    String referenceET = jobExecutionContext.containsKey("referenceET") ? jobExecutionContext
                            .getString("referenceET") : null;
                    String tradeET = jobExecutionContext.containsKey("tradeET")
                            ? jobExecutionContext
                            .getString("tradeET") : null;

                    System.out.println("************" + chunkContext.getStepContext().getStepExecution()
                            .getExecutionContext().get("final-step-state"));
                    chunkContext.getStepContext().getStepExecution().getExecutionContext()
                            .put("final-step-state", pricingET + " " + referenceET + " " + tradeET);

                    System.out.println(Thread.currentThread().getName() + " Final for job parameter : " +
                            chunkContext.getStepContext().getJobParameters().get(JOB_PARAMETER_KEY) + "" +
                            " and sending data :" + pricingET + ", " + tradeET + ", " + referenceET
                    );

                    // what happens when I retry a retry? - step execution lives across retries.
                    /*if(tradeET != null) {
                        throw new RuntimeException("Intentionally failing a retry");
                    }*/

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
                        if (subJobsFailed) {
                            stepExecution.setStatus(BatchStatus.FAILED);
                            stepExecution.setExitStatus(ExitStatus.FAILED);
                            return ExitStatus.FAILED;
                        }
                        return stepExecution.getExitStatus();
                    }
                })
                .build();
    }

    private Flow[] getDatasetFromConfiguration() {
        List<Flow> flows = new ArrayList<>();
        List<DataSyncJobMetadata> sortedMetadataByIncreasingRank =
                dataSyncJobDao.findAll()
                        .stream().sorted(Comparator.comparingInt(e -> e.getRank())).collect(Collectors.toList());

        System.out.println("Sorted ########## " + sortedMetadataByIncreasingRank);
        for (DataSyncJobMetadata metadata : sortedMetadataByIncreasingRank) {
            String dataset = metadata.getDataset();
            if (dataset.equals(PRICE)) {
                flows.add(pricingETFlow());
            } else if (dataset.equals(REFERENCE)) {
                flows.add(referenceETFlow());
            } else if (dataset.equals(TRADE)) {
                flows.add(tradeETFlow());
            } else {
                throw new IllegalStateException("Dataset " + dataset + " is not configured yet");
            }
        }
        Flow[] flowArray = new Flow[flows.size()];
        flowArray = flows.toArray(flowArray);
        return flowArray;
    }

    private FlowJobBuilder jobBuilder(String jobName) {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor(jobName + "-EXEC");

        Flow etlFlow = new FlowBuilder<Flow>("et-flow-split")
                .split(simpleAsyncTaskExecutor)
                .add(getDatasetFromConfiguration())
                .end();
        return jobBuilderFactory.get(jobName)
                .start(etlFlow)
                .on("COMPLETED")
                .to(finalStep())
                .from(etlFlow)
                .on("FAILED")
                .to(finalStep())
                .end();
    }

    @Bean
    @Qualifier("dataSync")
    public Job dataSync() throws Exception {
        System.out.println("datasync job initialized");
        return jobBuilder(DATA_SYNC_JOB_NAME)
                .incrementer(dataSyncJobParameterIncrementer())
                .build();
    }

    @Bean
    public DataSyncJobParameterIncrementer dataSyncJobParameterIncrementer() {
        return new DataSyncJobParameterIncrementer();
    }

    @Bean
    @Qualifier("dataSyncOverride")
    public Job dataSyncOverride() throws Exception {
        System.out.println("datasync job override job initialized");
        return jobBuilder(DATA_SYNC_OVERRIDE_JOB_NAME).build();
    }

    static class DataSyncJobParameterIncrementer implements JobParametersIncrementer {

        @Override
        public JobParameters getNext(JobParameters parameters) {
            JobParameter name = parameters.getParameters().get(JOB_PARAMETER_KEY);
            if (name == null) {
                System.out.println("First run");
                return new JobParametersBuilder().addString(JOB_PARAMETER_KEY, String.valueOf(0)).toJobParameters();
            }

            int previousCount = Integer.parseInt(String.valueOf(name.getValue()));
            System.out.println("Not first run. Previous count " + previousCount);
            return new JobParametersBuilder().addString(JOB_PARAMETER_KEY,
                    String.valueOf(previousCount + 1)).toJobParameters();
        }
    }

}