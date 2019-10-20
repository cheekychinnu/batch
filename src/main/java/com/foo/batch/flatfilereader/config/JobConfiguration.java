package com.foo.batch.flatfilereader.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.classify.Classifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.validation.BindException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class JobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    static class CustomerFieldSetMapper implements FieldSetMapper<Customer> {

        @Override
        public Customer mapFieldSet(FieldSet fieldSet) throws BindException {
            return new Customer(fieldSet.readString("id"),
                    fieldSet.readString("firstName"),
                    fieldSet.readString("lastName"));
        }
    }

    @Value("classpath:/data/customers*.csv")
    private Resource[] resources;

    @Bean
    public MultiResourceItemReader<Customer> multiResourceItemReader() {
        MultiResourceItemReader<Customer> reader = new MultiResourceItemReader<>();
        reader.setDelegate(customerFlatFileItemReader());
        reader.setResources(resources);
        return reader;
    }

    static class CustomerLineAggregator implements LineAggregator<Customer> {
        private ObjectMapper mapper = new ObjectMapper();

        @Override
        public String aggregate(Customer item) {
            try {
                return mapper.writeValueAsString(item);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Unable to serialize Customer", e);
            }
        }
    }

    static class CustomerClassifier implements Classifier<Customer, ItemWriter<? super Customer>> {

        private ItemWriter<Customer> evenWriter;
        private ItemWriter<Customer> oddWriter;

        CustomerClassifier(ItemWriter<Customer> evenWriter, ItemWriter<Customer> oddWriter) {
            this.evenWriter = evenWriter;
            this.oddWriter = oddWriter;
        }

        @Override
        public ItemWriter<? super Customer> classify(Customer classifiable) {
            if (Integer.parseInt(classifiable.getId()) % 2 == 0) {
                return evenWriter;
            }
            return oddWriter;
        }
    }

    @Bean
    public FlatFileItemWriter<Customer> customerOddFlatFileItemWriter() throws Exception {
        FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
        writer.setLineAggregator(new CustomerLineAggregator());
        String customerOutputPath = File.createTempFile("customerOutputOld", ".out").getAbsolutePath();
        System.out.println(">>> Output path : " + customerOutputPath);
        writer.setResource(new FileSystemResource(customerOutputPath));
        writer.afterPropertiesSet();
        return writer;
    }

    @Bean
    public FlatFileItemWriter<Customer> customerEvenFlatFileItemWriter() throws Exception {
        FlatFileItemWriter<Customer> writer = new FlatFileItemWriter<>();
        writer.setLineAggregator(new CustomerLineAggregator());
        String customerOutputPath = File.createTempFile("customerOutputEven", ".out").getAbsolutePath();
        System.out.println(">>> Output path : " + customerOutputPath);
        writer.setResource(new FileSystemResource(customerOutputPath));
        writer.afterPropertiesSet();
        return writer;
    }

    @Bean
    public CompositeItemWriter<Customer> customerCompositeItemWriter() throws Exception {
        List<ItemWriter<? super Customer>> writers = new ArrayList<>();
        writers.add(customerEvenFlatFileItemWriter());
        writers.add(customerOddFlatFileItemWriter());

        CompositeItemWriter<Customer> itemWriter = new CompositeItemWriter<>();
        itemWriter.setDelegates(writers);
        itemWriter.afterPropertiesSet();
        return itemWriter;
    }

    @Bean
    public ClassifierCompositeItemWriter<Customer> customerClassifierCompositeItemWriter() throws Exception {
        ClassifierCompositeItemWriter<Customer> itemWriter = new ClassifierCompositeItemWriter<>();
        itemWriter.setClassifier(new CustomerClassifier(customerEvenFlatFileItemWriter(), customerOddFlatFileItemWriter()));
        return itemWriter;
    }

    @Bean
    public FlatFileItemReader<Customer> customerFlatFileItemReader() {
        FlatFileItemReader<Customer> reader = new FlatFileItemReader<>();
        reader.setLinesToSkip(1); // to skip the header

        reader.setResource(new ClassPathResource("/data/customers.csv"));
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "firstName", "lastName");

        DefaultLineMapper<Customer> customerMapper = new DefaultLineMapper<>();
        customerMapper.setLineTokenizer(tokenizer);
        customerMapper.setFieldSetMapper(new CustomerFieldSetMapper());
        customerMapper.afterPropertiesSet();

        reader.setLineMapper(customerMapper);
        return reader;
    }

    @Bean
    public ItemWriter<Customer> customerItemWriter() {
        return items -> items.forEach(System.out::println);
    }

    @Bean
    public Step step1() throws Exception {

        // this is using simple flat file reader
        /*return stepBuilderFactory.get("step1")
                .<Customer, Customer>chunk(3)
                .reader(customerFlatFileItemReader())
                .writer(customerItemWriter())
                .build();*/

        // this is using multiple reader
        /*return stepBuilderFactory.get("step1")
                .<Customer, Customer>chunk(3)
                .reader(multiResourceItemReader())
                .writer(customerItemWriter())
                .build();*/

        // this is using multiple reader and writer
       /* return stepBuilderFactory.get("step1")
                .<Customer, Customer>chunk(3)
                .reader(multiResourceItemReader())
                .writer(customerCompositeItemWriter())
                .build();*/

        return stepBuilderFactory.get("step1")
                .<Customer, Customer>chunk(3)
                .reader(multiResourceItemReader())
                .writer(customerClassifierCompositeItemWriter())
                .stream(customerEvenFlatFileItemWriter())
                .stream(customerOddFlatFileItemWriter())
                .build();
    }

    @Bean
    public Job job() throws Exception {
        return jobBuilderFactory.get("job").start(step1()).build();
    }
}
