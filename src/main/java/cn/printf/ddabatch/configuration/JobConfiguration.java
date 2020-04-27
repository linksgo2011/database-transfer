package cn.printf.ddabatch.configuration;

import cn.printf.ddabatch.provider.MapSqlParameterSourceProvider;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.ColumnMapRowMapper;

import javax.sql.DataSource;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class JobConfiguration {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Qualifier("primary")
    @Autowired
    private DataSource dataSource;

    @Bean
    public Job transferDataJob(
            JobCompletionNotificationListener listener
    ) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(migrateUser())
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor("spring_batch");
    }

    @Value("${cli}")
    private String cli;

    public Step migrateUser() {
        return stepBuilderFactory.get("migrateUser")
                .<Map, Map>chunk(100)
                .reader(reader("select * from ct_person_info where COMPANY_CODE='testtant1' "))
                .processor(new ItemProcessor<Map, Map>() {
                    @Override
                    public Map process(Map item) throws Exception {
                        // 执行4次模拟
                        long start = System.currentTimeMillis();
                        for (int i = 0; i <4;i++){
                            System.out.println(cli + " cp /tmp/test.jpg  obs://avatar-dev/"+item.get("id")+"/custom.jpg -f");
                            Runtime.getRuntime().exec(cli + " cp /tmp/test.jpg  obs://avatar-dev/"+item.get("id")+"/custom.jpg -f");
                        }
                        long end = System.currentTimeMillis();
                        System.out.println("时间：" + (end-start));
                        item.put("HEAD_IMG_OBS", "custom.jpg");
                        System.out.println(item.get("id"));
                        return item;
                    }
                })
                .writer(writer("update ct_person_info set HEAD_IMG_OBS = :HEAD_IMG_OBS where PERSON_ACOUNT = :PERSON_ACOUNT and COMPANY_CODE = :COMPANY_CODE"))
                .taskExecutor(taskExecutor())
                .throttleLimit(1000)
                .build();
    }

    public JdbcCursorItemReader reader(String sql) {
        return new JdbcCursorItemReaderBuilder()
                .name("reader" + sql.hashCode())
                .rowMapper(new ColumnMapRowMapper())
                .sql(sql)
                .dataSource(dataSource)
                .build();
    }

    public JdbcBatchItemWriter<Map> writer(String sql) {
        JdbcBatchItemWriter<Map> writer = new JdbcBatchItemWriterBuilder<Map>()
                .itemSqlParameterSourceProvider(new MapSqlParameterSourceProvider())
                .sql(sql)
                .dataSource(dataSource)
                .build();
        writer.afterPropertiesSet();
        return writer;
    }
}
