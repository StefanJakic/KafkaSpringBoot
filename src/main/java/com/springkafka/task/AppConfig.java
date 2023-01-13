package com.springkafka.task;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;


@Configuration
public class AppConfig {
	
	@Bean
	public ProbeClass probeClassInfo() {
		
		return new ProbeClass();
	}
	
	
//	
//	@Bean
//	public ObjectMapper objectMapper{
//		ObjectMapper objM = new ObjectMapper();
//		
//		return objM;
//	}

}
