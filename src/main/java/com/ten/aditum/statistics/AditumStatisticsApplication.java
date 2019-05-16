package com.ten.aditum.statistics;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.ten.aditum.statistics.mapper")
public class AditumStatisticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AditumStatisticsApplication.class, args);
    }

}
