package com.foo.batch.localpartition.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
public class JobConfiguration {

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    static class MyPartitioner implements Partitioner {
        @Override
        public Map<String, ExecutionContext> partition(int gridSize) {
            Map<String, ExecutionContext> map = new HashMap<>();
            int count = 0;
            for (int i = 1; i <= 3; i++) {
                ExecutionContext context = new ExecutionContext();
                context.put("minValueInclusive", count);
                context.put("maxValueExclusive", count + 10);
                count += 10;
                map.put("Partition-" + i, context);
            }
            return map;
        }
    }

    @Bean
    public MyPartitioner partitioner() {
        MyPartitioner partitioner = new MyPartitioner();
        return partitioner;
    }

    @Bean
    @StepScope
    public ListItemReader<String> reader(@Value("#{stepExecutionContext['minValueInclusive']}") Integer minInclusive,
                                         @Value("#{stepExecutionContext['maxValueExclusive']}") Integer maxExclusive) {
        List<String> items = new ArrayList<>();
        System.out.println(Thread.currentThread().getName()+" Reading "+minInclusive+ " to "+maxExclusive);
        for (int i = minInclusive; i < maxExclusive; i++) {
            items.add(String.valueOf(i));
        }

        ListItemReader<String> reader = new ListItemReader<>(items);
        return reader;
    }

    @Bean
    @StepScope
    public ItemWriter<String> itemWriter() {
        return new ItemWriter<String>() {
            @Override
            public void write(List<? extends String> items) throws Exception {
                System.out.println(Thread.currentThread().getName()+" Writing chunk " + items.stream().collect(Collectors.joining(",")));
            }
        };
    }

    @Bean
    public Step slaveStep() {
        return stepBuilderFactory.get("slave")
                .<String, String>chunk(3)
                .reader(reader(null, null))
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .partitioner(slaveStep().getName(), partitioner())
                .step(slaveStep())
                .gridSize(4)
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .build();
    }

    @Bean
    public Job job() {
        return jobBuilderFactory.get("job")
                .start(step1())
                .build();
    }
}
