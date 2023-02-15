package com.myykk;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = {"com.myykk"})
@EnableScheduling
public class SchedulingVFSTasksApplication {

	public static void main(String[] args) {
		SpringApplication.run(SchedulingVFSTasksApplication.class);
	}
}
