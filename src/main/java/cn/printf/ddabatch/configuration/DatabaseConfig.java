package cn.printf.ddabatch.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

@Configuration
public class DatabaseConfig {

    @Primary
    @ConfigurationProperties(prefix = "spring.batch.datasource")
    @Bean(name = "batch")
    public DataSource batch() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "primary")
    @ConfigurationProperties(prefix = "spring.source.datasource")
    public DataSource sourceDb() {
        return DataSourceBuilder.create().build();
    }
}
