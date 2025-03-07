package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
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
public class ApiHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

	private AmazonDynamoDB dynamoDB;

	public ApiHandler(){
		// initialize client
		this.dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();
	}

	@Override
	public Map<String, Object> handleRequest(Map<String, Object> request, Context context) {
		String uuid = UUID.randomUUID().toString();
		String createdAt = ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT);

		context.getLogger().log("Received request: " + request);

		// create map to store event attributes for DynamoDB
		Map<String, AttributeValue> eventItem = new HashMap<>();
		eventItem.put("id", new AttributeValue(uuid));
		eventItem.put("principalId", new AttributeValue(request.get("principalId").toString()));
		eventItem.put("createdAt", new AttributeValue(createdAt));
		eventItem.put("body", new AttributeValue(request.get("content").toString()));

		context.getLogger().log("Content to be stored: " + request.get("content"));
		context.getLogger().log("Event: " + eventItem);

		// put item in DynamoDB table
		PutItemResult putItemResult = dynamoDB.putItem("Events", eventItem);
		context.getLogger().log("Put item result: " + putItemResult);

		// prepare response
		Map<String, Object> response = new HashMap<>();
		response.put("statusCode", 201);
		response.put("event", putItemResult);
		context.getLogger().log("Response: " + response);

		return response;
	}
}
