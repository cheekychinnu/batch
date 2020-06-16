package com.foo.batch.outbound.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.JobSynchronizationManager;
import org.springframework.batch.item.*;
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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Configuration
@Profile("outbound")
public class OutboundJobConfiguration extends DefaultBatchConfigurer implements ApplicationContextAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboundJobConfiguration.class);

    private static final Integer CHUNK_SIZE = 100;
    private static final String ACCOUNTING_PNL_BUNDLE = "accounting-pnl-bundle";
    private static final String ACCOUNTING_PNL_PORTFOLIO = "accounting-pnl-portfolio";

    @Autowired
    protected JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    protected JobExplorer jobExplorer;


    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobRegistry jobRegistry;

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

    <T> Step gearDatasetStepBuilder(
            String stepName, ItemReader<String> reader, ItemWriter<String>
            writer) {
        return stepBuilderFactory
                .get(stepName)
                .<String, String>chunk(CHUNK_SIZE)
                .reader(reader)
                .writer(writer)
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        JobSynchronizationManager.register(stepExecution.getJobExecution());
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        JobSynchronizationManager.release();
                        JobSynchronizationManager.close();
                        return stepExecution.getExitStatus();
                    }
                })
                .build();
    }

    @Bean
    public Step accountingPnlBundleStep() {
        String stepName = ACCOUNTING_PNL_BUNDLE + "-step";
        List<String> input = IntStream.range(1, 6).mapToObj(i -> stepName + i).collect(Collectors.toList());
        return gearDatasetStepBuilder(stepName, new ItemReader<String>() {
            private Iterator<String> iterator = input.iterator();

            @Override
            public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                return null;
            }
        }, items -> LOGGER.info(String.join(",", items)));
    }

    @Bean
    public Flow accountingPnlBundleFlow() {
        return new FlowBuilder<Flow>(ACCOUNTING_PNL_BUNDLE + "-flow")
                .start(accountingPnlBundleStep())
                .end();
    }

    @Autowired
    private AccountingPnlReader accountingPnlReader;

    @Bean
    public Step accountingPnlPortfolioStep() {
        String stepName = ACCOUNTING_PNL_PORTFOLIO + "-step";
        return gearDatasetStepBuilder(stepName,accountingPnlReader , items -> LOGGER.info(String.join(",", items)));
    }

    @Bean
    public Flow accountingPnlPortfolioFlow() {
        return new FlowBuilder<Flow>(ACCOUNTING_PNL_PORTFOLIO + "-flow")
                .start(accountingPnlPortfolioStep())
                .end();
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutorForGearETLSteps() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();

        threadPoolTaskExecutor.setCorePoolSize(15);
        threadPoolTaskExecutor.setMaxPoolSize(20);
        threadPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        threadPoolTaskExecutor.setThreadNamePrefix("GEAR-SYNC-EXEC");
        return threadPoolTaskExecutor;
    }

    public Flow[] getGearAccountingPnlFlows() {
        List<Flow> flows = new ArrayList<>();
        flows.add(accountingPnlBundleFlow());
        flows.add(accountingPnlPortfolioFlow());
        Flow[] flowArray = new Flow[flows.size()];
        flowArray = flows.toArray(flowArray);
        return flowArray;
    }

    @Bean
    public Flow updatePnlRunTableFlow() {
        return new FlowBuilder<Flow>("pnl-run" + "-flow")
                .start(updatePnlRunTableStep())
                .end();
    }

    @Bean
    public Step updatePnlRunTableStep() {
        return stepBuilderFactory.get("pnl-run")
                .tasklet((contribution, chunkContext) -> {
                    Thread.sleep(2000);
                    LOGGER.info("updating pnl run table");
//                    throwException();
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Flow updateBookBundleBusinessUnitTableFlow() {
        return new FlowBuilder<Flow>("book-bundle-bu" + "-flow")
                .start(updateBookBundleBusinessUnitTableStep())
                .end();
    }

    @Bean
    public Step updateBookBundleBusinessUnitTableStep() {
        return stepBuilderFactory.get("book-bundle-bu")
                .tasklet((contribution, chunkContext) -> {
                    Thread.sleep(2000);
                    LOGGER.info("updating book bundle bu table");
                    throwException();
                    return RepeatStatus.FINISHED;
                }).build();
    }

    private void throwException() {
        if (true) {
            throw new IllegalStateException("intentional exception");
        }
    }

    @Bean
    public Flow updateSpnGboTypeTableFlow() {
        return new FlowBuilder<Flow>("spn-gbo-type" + "-flow")
                .start(updateSpnGboTypeTableStep())
                .end();
    }

    @Bean
    public Step updateSpnGboTypeTableStep() {
        return stepBuilderFactory.get("spn-gbo-type")
                .tasklet((contribution, chunkContext) -> {
                    Thread.sleep(2000);
                    LOGGER.info("updating spn gbo type table");
                    return RepeatStatus.FINISHED;
                })
                .listener(new StepExecutionListener() {
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        throwException();
                    }

                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        return stepExecution.getExitStatus();
                    }
                })
                .build();
    }

    @Bean
    public Flow updateCommonTablesForAccountingPnlFlow() {
        List<Flow> flows = new ArrayList<>();
        flows.add(updateSpnGboTypeTableFlow());
        flows.add(updateBookBundleBusinessUnitTableFlow());
        Flow[] flowArray = new Flow[flows.size()];
        flowArray = flows.toArray(flowArray);
        Flow otherCommonTables = new FlowBuilder<Flow>("other-tables")
                .split(taskExecutorForGearETLSteps())
                .add(flowArray)
                .end();
        Flow flow = new FlowBuilder<Flow>("update-common-accounting-pnl-tables-flow")
                .start(updatePnlRunTableFlow())
                .on("COMPLETED")
                .to(otherCommonTables)
                .from(updatePnlRunTableFlow())
                .on("FAILED")
                .to(rollbackGearChanges())
                .end();
        return flow;
    }

    @Bean
    public Step rollbackGearChanges() {
        return stepBuilderFactory.get("rollback")
                .tasklet((contribution, chunkContext) -> {
                    Thread.sleep(2000);
                    LOGGER.info("rollback");
//                                        throwException();
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step determineQueryKtRange(AccountingPnlParameter accountingPnlParameter) {
        return stepBuilderFactory.get("determine-kt-range")
                .tasklet((contribution, chunkContext) -> {
                    accountingPnlParameter.setParameter("something"+new Date());
                    LOGGER.info("determine-kt-range");
                    Thread.sleep(10000);
//                    throwException();
//                    chunkContext.getStepContext().getStepExecution().setExitStatus(new ExitStatus("NOOP"));
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    @Qualifier("gearAccountingPnlSync")
    public Job gearAccountingPnlSync() {

        String jobName = "gear-accounting-pnl-sync";

        Flow etlFlow =
                new FlowBuilder<Flow>("dataset-etl-flow-split")
                        .split(taskExecutorForGearETLSteps())
                        .add(getGearAccountingPnlFlows())
                        .end();
        return jobBuilderFactory
                .get(jobName)
                .incrementer(new RunIdIncrementer())
                .start(determineQueryKtRange(null)).on("COMPLETED").to(etlFlow)
                .from(determineQueryKtRange(null)).on("FAILED").fail()
                .from(determineQueryKtRange(null)).on("NOOP").end()
                .from(etlFlow).on("COMPLETED").to(updateCommonTablesForAccountingPnlFlow())
                .from(etlFlow).on("FAILED").to(rollbackGearChanges())
                .end()
                .listener(new JobExecutionListener() {

                    @Override
                    public void beforeJob(JobExecution jobExecution) {

                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
//                        throwException();
                        List<StepExecution> failedSteps = jobExecution.getStepExecutions().stream()
                                .filter(s -> s.getExitStatus().getExitCode().equals(ExitStatus.FAILED.getExitCode()))
                                .collect(Collectors.toList());
                        if (!failedSteps.isEmpty()) {
                            boolean onlyUnImportantCommonTableUpdatedFailed = failedSteps.stream().map(StepExecution::getStepName).allMatch(n -> n.equals("spn-gbo-type") ||
                                    n.equals("book-bundle-bu"));
                            boolean rollbackItselfFailed = failedSteps.stream().anyMatch(s -> s.getStepName().equals("rollback"));
                            if (onlyUnImportantCommonTableUpdatedFailed) {
                                jobExecution.setExitStatus(new ExitStatus("FAILED_AT_UNIMPORTANT_COMMON_TABLES"));
                            } else if (rollbackItselfFailed) {
                                jobExecution.setExitStatus(new ExitStatus("FAILED_AT_ROLLBACK"));
                            } else if (jobExecution.getStepExecutions().stream().anyMatch(e -> e.getStepName().equals("rollback"))) {
                                jobExecution.setExitStatus(new ExitStatus("FAILED_BUT_ROLLED_BACK"));
                                jobExecution.setStatus(BatchStatus.FAILED);
                            }
                        }
                    }
                })
                .preventRestart()
                .build();
    }
}
