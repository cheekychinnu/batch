package com.foo.batch.itemprocessor.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.foo.batch.flatfilereader.config.Customer;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.validator.ValidatingItemProcessor;
import org.springframework.batch.item.validator.ValidationException;
import org.springframework.batch.item.validator.Validator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.validation.BindException;

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

    static class UpperCaseItemProcessor implements ItemProcessor<Customer, Customer> {

        @Override
        public Customer process(Customer item) throws Exception {
            return new Customer(item.getId(), item.getFirstName().toUpperCase(), item.getLastName().toUpperCase());
        }
    }

    static class CustomerValidator implements Validator<Customer> {

        @Override
        public void validate(Customer value) throws ValidationException {
            if (value.getFirstName().equalsIgnoreCase("dwight")) {
                throw new ValidationException("I dont like Dwight");
            }
        }
    }

    @Bean
    public ValidatingItemProcessor<Customer> customerValidatingItemProcessor() {
        ValidatingItemProcessor<Customer> customerValidatingItemProcessor = new ValidatingItemProcessor<>(new CustomerValidator());
        customerValidatingItemProcessor.setFilter(true); // if this is not set, any validation error will fail your entire job
        return customerValidatingItemProcessor;
    }
    @Bean
    public ItemProcessor<Customer,Customer> upperCaseItemProcessor() {
        return new UpperCaseItemProcessor();
    }

    @Bean
    public ItemWriter<Customer> customerItemWriter() {
        return items -> items.forEach(System.out::println);
    }

    @Bean
    public Step step1() throws Exception {

        // this is using simple flat file reader
        return stepBuilderFactory.get("step1")
                .<Customer, Customer>chunk(3)
                .reader(customerFlatFileItemReader())
//                .processor(upperCaseItemProcessor())
                .processor(customerValidatingItemProcessor())
                .writer(customerItemWriter())
                .build();
    }


    @Bean
    public Job job() throws Exception {
        return jobBuilderFactory.get("job").start(step1()).build();
    }
}
