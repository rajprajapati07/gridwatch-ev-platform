package com.gridwatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class GridWatchApplication {
    public static void main(String[] args) {
        SpringApplication.run(GridWatchApplication.class, args);
    }
}
