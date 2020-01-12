package cn.printf.ddabatch.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class JobConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Qualifier("mysqlDb")
    @Autowired
    private  DataSource mysqlDb;

    @Qualifier("gaussDb")
    @Autowired
    private  DataSource gaussDb;

    @Bean
    public Job transferDataJob(JobCompletionNotificationListener listener, Step step1) {
        return jobBuilderFactory.get("importUserJob")
                .preventRestart()
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<Map, Map>chunk(10)
                .reader(reader())
                .writer(writer())
                .build();
    }

    public JdbcCursorItemReader reader() {
        // TODO 检测表列名，然后加上 HEX

        return new JdbcCursorItemReaderBuilder()
                .name("reader")
                .rowMapper((rs, rowNum) -> {
                    return new HashMap<>();
                })
                .sql("select * from users")
                .dataSource(gaussDb)
                .build();
    }

    public JdbcBatchItemWriter<Map> writer() {
        return new JdbcBatchItemWriterBuilder<Map>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO  (first_name, last_name) VALUES (:firstName, :lastName)")
                .dataSource(gaussDb)
                .build();
    }
}
