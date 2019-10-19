package com.foo.batch.statefulreader.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class JobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    static class StatefulItemReader implements ItemStreamReader<String> {

        private boolean restart = false;
        private int currIndex = 0;
        private List<String> items;

        StatefulItemReader(List<String> items) {
            this.items = items;
            this.currIndex = 0;
        }

        @Override
        public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
            String item = null;
            if (currIndex < items.size()) {
                item = this.items.get(currIndex);
                currIndex++;
            }
            if (this.currIndex == 4 && !restart) {
                throw new RuntimeException("Simulated exception");
            }
            return item;
        }

        @Override
        public void open(ExecutionContext executionContext) throws ItemStreamException {
            if (executionContext.containsKey("currIndex")) {
                this.restart = true;
                this.currIndex = executionContext.getInt("currIndex");
            } else {
                this.currIndex = 0;
                executionContext.put("currIndex", currIndex);
            }
        }

        @Override
        public void update(ExecutionContext executionContext) throws ItemStreamException {
            executionContext.put("currIndex", currIndex);
        }

        @Override
        public void close() throws ItemStreamException {

        }
    }

    @Bean
    @StepScope
    public ItemStreamReader<String> itemReader() {
        List<String> items = new ArrayList<>(100);
        for (int i = 1; i <= 100; i++) {
            items.add(String.valueOf(i));
        }

        return new StatefulItemReader(items);
    }

    @Bean
    public ItemWriter<String> itemWriter() {
        return items -> {
            for (String i : items) {
                System.out.println(i);
            }
        };
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<String,String>chunk(2)
                .reader(itemReader())
                .writer(itemWriter())
                .stream(itemReader())
                .build();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("job")
                .start(step1())
                .build();
    }
}
