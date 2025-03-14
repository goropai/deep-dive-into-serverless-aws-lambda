package com.task10;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.google.gson.JsonObject;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DynamoDBPersister {
    private final AmazonDynamoDB amazonDynamoDB;

    public DynamoDBPersister(AmazonDynamoDB amazonDynamoDB) {
        this.amazonDynamoDB = amazonDynamoDB;
    }

    public void persistData(JsonObject weatherData, String targetTable, LambdaLogger logger) {
        logger.log("Persisting data to DynamoDB table: " + targetTable);
        Map<String, AttributeValue> attributesMap = new HashMap<>();
        attributesMap.put("id", new AttributeValue(UUID.randomUUID().toString()));
        attributesMap.put("forecast", new AttributeValue().withS(weatherData.toString()));
        amazonDynamoDB.putItem(targetTable, attributesMap);
        logger.log("Data saved: " + attributesMap);
    }
}
