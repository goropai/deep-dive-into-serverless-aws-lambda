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

		// create map to store event attributes for DynamoDB
		Map<String, AttributeValue> eventItem = new HashMap<>();
		eventItem.put("id", new AttributeValue(uuid));
		eventItem.put("principalId", new AttributeValue(request.get("principalId").toString()));
		eventItem.put("createdAt", new AttributeValue(createdAt));
		eventItem.put("body", new AttributeValue(request.get("content").toString()));  // make sure 'content' exists in the request

		// put item in DynamoDB table
		PutItemResult putItemResult = dynamoDB.putItem("Events", eventItem);

		// prepare response
		Map<String, Object> response = new HashMap<>();
		response.put("statusCode", 201);
		Map<String, Object> event = new HashMap<>();
		event.put("id", uuid);
		event.put("principalId", request.get("principalId"));
		event.put("createdAt", createdAt);
		event.put("body", request.get("content"));
		response.put("event", event);

		return response;
	}
}
