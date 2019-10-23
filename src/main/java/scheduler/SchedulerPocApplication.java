package scheduler;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableBatchProcessing
@EnableScheduling
@PropertySource(ignoreResourceNotFound=false,value="classpath:application-scheduler.properties")
public class SchedulerPocApplication {
    public static void main(String[] args) {
        SpringApplication.run(SchedulerPocApplication.class, args);
    }
}
