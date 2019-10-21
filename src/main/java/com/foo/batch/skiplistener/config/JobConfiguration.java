package com.foo.batch.skiplistener.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class JobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    static class SkipItemWriter implements ItemWriter<String> {

        @Override
        public void write(List<? extends String> items) throws Exception {
            for (String item : items) {
                System.out.println("Writing item " + item);
                if (item.equalsIgnoreCase("-84")) {
                    System.out.println("Writing item " + item + " failed");
                    throw new CustomRetryableException("Write failed. Attempt ");
                } else {
                    System.out.println(item);
                }
            }
        }
    }

    static class SkipItemProcessor implements ItemProcessor<String, String> {

        @Override
        public String process(String item) throws Exception {
            System.out.println("Processing item " + item);
            if (item.equalsIgnoreCase("42")) {
                System.out.println("Processing item "+item+" failed");
                throw new CustomRetryableException("Process failed. Attemp ");

            } else {
                return String.valueOf(Integer.valueOf(item) * -1);
            }
        }

    }

    @Bean
    @StepScope
    public ListItemReader<String> reader() {
        List<String> items = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            items.add(String.valueOf(i));
        }

        ListItemReader<String> reader = new ListItemReader<>(items);
        return reader;
    }

    @Bean
    @StepScope
    public SkipItemProcessor processor(@Value("#{jobParameters['skip']}") String skip) {
        SkipItemProcessor processor = new SkipItemProcessor();

        return processor;
    }

    @Bean
    @StepScope
    public SkipItemWriter writer(@Value("#{jobParameters['skip']}") String skip) {
        SkipItemWriter writer = new SkipItemWriter();
        return writer;
    }

    static class CustomRetryableException extends Exception {
        CustomRetryableException() {
            super();
        }

        CustomRetryableException(String message) {
            super(message);
        }
    }

    static class CustomSkipListener implements SkipListener {

        @Override
        public void onSkipInRead(Throwable t) {

        }

        @Override
        public void onSkipInWrite(Object item, Throwable t) {
            System.out.println(">> skipping item " + item + " because writing caused " + t.getMessage());
        }

        @Override
        public void onSkipInProcess(Object item, Throwable t) {
            System.out.println(">> skipping item " + item + " because processing caused " + t.getMessage());
        }
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<String, String>chunk(10)
                .reader(reader())
                .processor(processor(null))
                .writer(writer(null))
                .faultTolerant()
                .skip(CustomRetryableException.class)
                .skipLimit(15)
                .listener(new CustomSkipListener())
                .build();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("job1")
                .start(step1())
                .build();
    }
}
