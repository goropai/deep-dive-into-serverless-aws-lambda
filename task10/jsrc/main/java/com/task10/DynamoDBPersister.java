package com.task10;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.*;

public class DynamoDBPersister {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void persistData(JsonObject weatherData, String targetTable, LambdaLogger logger) {
        logger.log("Persisting data to DynamoDB table: " + targetTable);
        try (DynamoDbClient client = DynamoDbClient.builder().build()) {
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(targetTable)
                    .item(Map.of(
                            "id", AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
                            "forecast", AttributeValue.builder().m(getData(objectMapper.readTree(weatherData.getAsString()))).build()
                    ))
                    .build();
            client.putItem(request);
            logger.log("Data saved: " + request.item());
        } catch (Exception e) {
            logger.log("Error saving data: " + e.getMessage());
        }
    }

    private Map<String, AttributeValue> getData(JsonNode weatherNode) {
        Map<String, AttributeValue> item = new HashMap<>();

        item.put("elevation", AttributeValue.builder().n(weatherNode.get("elevation").asText()).build());
        item.put("generationtime_ms", AttributeValue.builder().n(weatherNode.get("generationtime_ms").asText()).build());
        item.put("latitude", AttributeValue.builder().n(weatherNode.get("latitude").asText()).build());
        item.put("longitude", AttributeValue.builder().n(weatherNode.get("longitude").asText()).build());
        item.put("timezone", AttributeValue.builder().s(weatherNode.get("timezone").asText()).build());
        item.put("timezone_abbreviation", AttributeValue.builder().s(weatherNode.get("timezone_abbreviation").asText()).build());
        item.put("utc_offset_seconds", AttributeValue.builder().n(weatherNode.get("utc_offset_seconds").asText()).build());

        // Handle "hourly"
        Map<String, AttributeValue> hourlyMap = new HashMap<>();

        // Handle "temperature_2m" - Convert JsonNode array to List<String>
        List<String> temperature2mList = new ArrayList<>();
        JsonNode temperature2mArray = weatherNode.get("hourly").get("temperature_2m");
        if (temperature2mArray.isArray()) {
            for (JsonNode node : temperature2mArray) {
                temperature2mList.add(node.asText());
            }
        }
        hourlyMap.put("temperature_2m", AttributeValue.builder().ns(temperature2mList).build());

        // Handle "time" - Convert JsonNode array to List<String>
        List<String> timeList = new ArrayList<>();
        JsonNode timeArray = weatherNode.get("hourly").get("time");
        if (timeArray.isArray()) {
            for (JsonNode node : timeArray) {
                timeList.add(node.asText());
            }
        }
        hourlyMap.put("time", AttributeValue.builder().ss(timeList).build()); // Use 'ss' for String Set

        item.put("hourly", AttributeValue.builder().m(hourlyMap).build());

        // Handle "hourly_units"
        Map<String, AttributeValue> hourlyUnitsMap = new HashMap<>();
        hourlyUnitsMap.put("temperature_2m", AttributeValue.builder().s(weatherNode.get("hourly_units").get("temperature_2m").asText()).build());
        hourlyUnitsMap.put("time", AttributeValue.builder().s(weatherNode.get("hourly_units").get("time").asText()).build());
        item.put("hourly_units", AttributeValue.builder().m(hourlyUnitsMap).build());

        return item;
    }
}
