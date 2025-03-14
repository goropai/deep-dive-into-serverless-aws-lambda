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
            JsonNode weatherNode = objectMapper.readTree(weatherData.toString()); // Parse JsonObject to JsonNode
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(targetTable)
                    .item(Map.of(
                            "id", AttributeValue.builder().s(UUID.randomUUID().toString()).build(),
                            "forecast", AttributeValue.builder().m(getData(weatherNode)).build()
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

        item.put("latitude", AttributeValue.builder().n(weatherNode.get("latitude").asText()).build());
        item.put("longitude", AttributeValue.builder().n(weatherNode.get("longitude").asText()).build());
        item.put("generationtime_ms", AttributeValue.builder().n(weatherNode.get("generationtime_ms").asText()).build());
        item.put("utc_offset_seconds", AttributeValue.builder().n(weatherNode.get("utc_offset_seconds").asText()).build());
        item.put("timezone", AttributeValue.builder().s(weatherNode.get("timezone").asText()).build());
        item.put("timezone_abbreviation", AttributeValue.builder().s(weatherNode.get("timezone_abbreviation").asText()).build());
        item.put("elevation", AttributeValue.builder().n(weatherNode.get("elevation").asText()).build());

        // Handle current_units
        item.put("current_units", AttributeValue.builder().m(getNestedMap(weatherNode, "current_units")).build());

        // Handle current
        item.put("current", AttributeValue.builder().m(getNestedMap(weatherNode, "current")).build());

        // Handle hourly_units
        item.put("hourly_units", AttributeValue.builder().m(getNestedMap(weatherNode, "hourly_units")).build());

        // Handle hourly
        item.put("hourly", AttributeValue.builder().m(getHourlyMap(weatherNode, "hourly")).build());

        return item;
    }

    private Map<String, AttributeValue> getNestedMap(JsonNode weatherNode, String fieldName) {
        Map<String, AttributeValue> nestedMap = new HashMap<>();
        JsonNode node = weatherNode.get(fieldName);

        if (node != null && node.isObject()) {
            node.fields().forEachRemaining(entry -> {
                String key = entry.getKey();
                JsonNode valueNode = entry.getValue();

                if (valueNode.isTextual()) {
                    nestedMap.put(key, AttributeValue.builder().s(valueNode.asText()).build());
                } else if (valueNode.isNumber()) {
                    nestedMap.put(key, AttributeValue.builder().n(valueNode.asText()).build());
                }
            });
        }
        return nestedMap;
    }

    private Map<String, AttributeValue> getHourlyMap(JsonNode weatherNode, String fieldName) {
        Map<String, AttributeValue> hourlyMap = new HashMap<>();
        JsonNode hourlyNode = weatherNode.get(fieldName);

        if (hourlyNode != null && hourlyNode.isObject()) {
            // Handle "time" - Convert JsonNode array to List<String>
            List<String> timeList = new ArrayList<>();
            JsonNode timeArray = hourlyNode.get("time");
            if (timeArray != null && timeArray.isArray()) {
                for (JsonNode node : timeArray) {
                    timeList.add(node.asText());
                }
                hourlyMap.put("time", AttributeValue.builder().ss(timeList).build()); // Use 'ss' for String Set
            }

            // Handle "temperature_2m" - Convert JsonNode array to List<String>
            List<String> temperature2mList = new ArrayList<>();
            JsonNode temperature2mArray = hourlyNode.get("temperature_2m");
            if (temperature2mArray != null && temperature2mArray.isArray()) {
                for (JsonNode node : temperature2mArray) {
                    temperature2mList.add(node.asText());
                }
                hourlyMap.put("temperature_2m", AttributeValue.builder().ns(temperature2mList).build());
            }

            // Handle "relative_humidity_2m" - Convert JsonNode array to List<String>
            List<String> relativeHumidity2mList = new ArrayList<>();
            JsonNode relativeHumidity2mArray = hourlyNode.get("relative_humidity_2m");
            if (relativeHumidity2mArray != null && relativeHumidity2mArray.isArray()) {
                for (JsonNode node : relativeHumidity2mArray) {
                    relativeHumidity2mList.add(node.asText());
                }
                hourlyMap.put("relative_humidity_2m", AttributeValue.builder().ns(relativeHumidity2mList).build());
            }

            // Handle "wind_speed_10m" - Convert JsonNode array to List<String>
            List<String> windSpeed10mList = new ArrayList<>();
            JsonNode windSpeed10mArray = hourlyNode.get("wind_speed_10m");
            if (windSpeed10mArray != null && windSpeed10mArray.isArray()) {
                for (JsonNode node : windSpeed10mArray) {
                    windSpeed10mList.add(node.asText());
                }
                hourlyMap.put("wind_speed_10m", AttributeValue.builder().ns(windSpeed10mList).build());
            }
        }

        return hourlyMap;
    }
}
