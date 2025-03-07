package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
    lambdaName = "api_handler",
	roleName = "api_handler-role",
	isPublishVersion = true,
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariable(key = "target_table", value = "${target_table}")
public class ApiHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {
	private final String tableName;
	private final ObjectMapper objectMapper = new ObjectMapper();

	public ApiHandler() {
		this.tableName = System.getenv("target_table");
	}

	@Override
	public Map<String, Object> handleRequest(Map<String, Object> request, Context context) {
		AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
		String uuid = UUID.randomUUID().toString();
		String createdAt = ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT);

		context.getLogger().log("Table name: " + tableName);
		context.getLogger().log("Received request: " + request);

		Map<String, Object> bodyMap;
        try {
            bodyMap = objectMapper.readValue((String) request.get("body"), HashMap.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        // create map to store event attributes for DynamoDB
		Map<String, AttributeValue> eventItem = new HashMap<>();
		eventItem.put("id", new AttributeValue(uuid));
		eventItem.put("principalId", new AttributeValue(bodyMap.get("principalId").toString()));
		eventItem.put("createdAt", new AttributeValue(createdAt));
		eventItem.put("body", new AttributeValue(bodyMap.get("content").toString()));

		context.getLogger().log("Content to be stored: " + bodyMap.get("content"));
		context.getLogger().log("Event: " + eventItem);

		// put item in DynamoDB table
		PutItemResult putItemResult = dynamoDB.putItem(new PutItemRequest(tableName, eventItem));
		Map<String, AttributeValue> key = new HashMap<>();
		key.put("id", new AttributeValue(uuid));
		GetItemResult result = dynamoDB.getItem(new GetItemRequest(tableName, key));
		context.getLogger().log("Item from database: " + result.getItem());

		// prepare response
		Map<String, Object> response = new HashMap<>();
		response.put("statusCode", 201);
		response.put("event", result.getItem());
		context.getLogger().log("Response: " + response);

		return response;
	}
}
