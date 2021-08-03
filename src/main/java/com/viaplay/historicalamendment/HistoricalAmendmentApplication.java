package com.viaplay.historicalamendment;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class HistoricalAmendmentApplication {

    @Bean
    public AmazonDynamoDB dynamoClient(){
        return AmazonDynamoDBClientBuilder
                .standard()
                .withRegion("eu-west-1")
                .build();
    }

    @Bean
    public DynamoDB getDynamoDb() {
        return new DynamoDB(dynamoClient());
    }

    public static void main(String[] args) {
        SpringApplication.run(HistoricalAmendmentApplication.class, args);
    }

}
