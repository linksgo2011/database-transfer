package cn.printf.ddabatch.configuration;

import cn.printf.ddabatch.provider.MapSqlParameterSourceProvider;
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
import org.springframework.jdbc.core.ColumnMapRowMapper;

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
    private DataSource mysqlDb;

    @Qualifier("gaussDb")
    @Autowired
    private DataSource gaussDb;

    @Bean
    public Job transferDataJob(
            JobCompletionNotificationListener listener
    ) {
        return jobBuilderFactory.get("importUserJob")
                .preventRestart()
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(migrateUser())
                .next(migrateUserBinary())
                .build();
    }

    public Step migrateUser() {
        return stepBuilderFactory.get("migrateUser")
                .<Map, Map>chunk(10)
                .reader(reader("select * from users"))
                .writer(writer("INSERT INTO  USERS(ID, NAME) VALUES (:ID, :NAME)"))
                .build();
    }

    public Step migrateUserBinary() {
        return stepBuilderFactory.get("migrateUser")
                .<Map, Map>chunk(10)
                .reader(reader("select * from USERS_BINARY_ID"))
                .writer(writer("INSERT INTO USERS_BINARY_ID(ID, NAME) VALUES (:ID, :NAME)"))
                .build();
    }

    public JdbcCursorItemReader reader(String sql) {
        return new JdbcCursorItemReaderBuilder()
                .name("reader" + sql.hashCode())
                .rowMapper(new ColumnMapRowMapper())
                .sql(sql)
                .dataSource(mysqlDb)
                .build();
    }

    public JdbcBatchItemWriter<Map> writer(String sql) {
        JdbcBatchItemWriter<Map> writer = new JdbcBatchItemWriterBuilder<Map>()
                .itemSqlParameterSourceProvider(new MapSqlParameterSourceProvider())
                .sql(sql)
                .dataSource(gaussDb)
                .build();
        writer.afterPropertiesSet();
        return writer;
    }
}
