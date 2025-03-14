package com.task10;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.google.gson.JsonObject;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;
import java.util.UUID;

public class DynamoDBPersister {

    public void persistData(JsonObject weatherData, String targetTable, LambdaLogger logger) {
        logger.log("Persisting data to DynamoDB table: " + targetTable);
        try (DynamoDbClient client = DynamoDbClient.builder().build()) {
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(targetTable)
                    .item(Map.of(
                            "id", AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
                            "forecast", AttributeValue.builder().s(weatherData.getAsString()).build()
                    ))
                    .build();
            client.putItem(request);
            logger.log("Data saved: " + request.item());
        }
        catch (Exception e) {
            logger.log("Error saving data: " + e.getMessage());
        }
    }
}
