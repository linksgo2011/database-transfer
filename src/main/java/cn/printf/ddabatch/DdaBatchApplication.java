package cn.printf.ddabatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties
public class DdaBatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(DdaBatchApplication.class, args);
	}
}
