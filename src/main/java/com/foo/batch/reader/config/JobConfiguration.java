package com.foo.batch.reader.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Iterator;
import java.util.List;

@Configuration
public class JobConfiguration {

    static class StatelessItemReader implements ItemReader<String> {

        private final Iterator<String> data;

        StatelessItemReader(List<String> names) {
            this.data = names.iterator();
        }

        @Override
        public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
            System.out.println("Reader invoked");
            if(data.hasNext()) {
                return data.next();
            }
            return null;
        }
    }

    @Bean
    public StatelessItemReader statelessItemReader() {
        return new StatelessItemReader(List.of("Mike","Jim","Pam", "Dwight","Holly","Jan"));
    }

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<String,String>chunk(2) // buffers 2 elements before sending to writer
                .reader(statelessItemReader())
                .writer(items ->{
                    System.out.println("Writer invoked");
                    items.forEach(
                            e -> System.out.println(e)
                    );
                } ).build();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("readerJob").start(step1()).build();
    }
}
