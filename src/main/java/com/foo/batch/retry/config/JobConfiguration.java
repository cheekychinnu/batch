package com.foo.batch.retry.config;

import org.springframework.batch.core.Job;
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

    @Bean
    @StepScope
    public ListItemReader<String> reader() {
        List<String> items = new ArrayList<>();

        for(int i=0;i<100;i++) {
            items.add(String.valueOf(i));
        }

        ListItemReader<String> reader = new ListItemReader<>(items);
        return reader;
    }

    static class RetryItemProcessor implements ItemProcessor<String,String> {

        private boolean retry = false;
        private int attempCount = 0;

        @Override
        public String process(String item) throws Exception {
            System.out.println("Processing item "+item);
            if(retry && item.equalsIgnoreCase("42")) {
                attempCount++;
                if(attempCount >= 5) {
                    System.out.println("Success");
                    retry = false;
                    return String.valueOf(Integer.valueOf(item)* -1);
                } else {
                    System.out.println("Processing of item "+ item+ " failed");
                    throw  new CustomRetryableException("Process failed. Attemp "+attempCount);
                }
            } else {
                return String.valueOf(Integer.valueOf(item) * -1);
            }
        }

        public void setRetry(boolean retry) {
            this.retry = retry;
        }

    }

    static class RetryItemWriter implements ItemWriter<String> {
        private boolean retry = false;
        private int attempCount = 0;

        @Override
        public void write(List<? extends String> items) throws Exception {
            for(String item: items) {
                System.out.println("Writing item " + item);
                if (retry && item.equalsIgnoreCase("-84")) {
                    attempCount++;
                    if (attempCount >= 5) {
                        System.out.println("Success");
                        retry = false;
                        System.out.println(item);
                    } else {
                        System.out.println("Writing item " + item + " failed");
                        throw new CustomRetryableException("Write failed. Attempt " + attempCount);
                    }
                } else {
                    System.out.println(item);
                }

            }
        }

        public void setRetry(boolean retry) {
            this.retry = retry;
        }
    }

    @Bean
    @StepScope
    public RetryItemProcessor processor(@Value("#{jobParameters['retry']}") String retry) {
        RetryItemProcessor processor = new RetryItemProcessor();
        processor.setRetry(StringUtils.hasText(retry) && retry.equalsIgnoreCase("processor"));
        return processor;
    }

    @Bean
    @StepScope
    public RetryItemWriter writer(@Value("#{jobParameters['retry']}") String retry) {
        RetryItemWriter retryItemWriter = new RetryItemWriter();
        retryItemWriter.setRetry(StringUtils.hasText(retry) && retry.equalsIgnoreCase("writer"));
        return retryItemWriter;
    }

    static class CustomRetryableException extends Exception {
        CustomRetryableException() {
            super();
        }

        CustomRetryableException(String message) {
            super(message);
        }
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<String,String>chunk(10)
                .reader(reader())
                .processor(processor(null))
                .writer(writer(null))
                .faultTolerant()
                .retry(CustomRetryableException.class)
                .retryLimit(15)
                .build();
    }


    @Bean
    public Job job() {
        return jobBuilderFactory.get("job")
                .start(step1()).build();
    }
}
