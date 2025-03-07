package com.task05;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.time.Instant;
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
@EnvironmentVariable(
		key = "target_table", value = "${target_table}"
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
public class ApiHandler implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {
	private final ObjectMapper objectMapper = new ObjectMapper();

	@Override
	public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent event, Context context) {
		try {
			context.getLogger().log("Received event: " + objectMapper.writeValueAsString(event));

			String body = event.getBody();
			if (body == null || body.isEmpty()) {
				throw new RuntimeException("Empty request body");
			}

			JsonNode requestBody = objectMapper.readTree(body);
			JsonNode principalIdNode = requestBody.get("principalId");
			int principalId = principalIdNode.asInt();

			JsonNode content = requestBody.get("content");

			String eventId = UUID.randomUUID().toString();
			String createdAt = Instant.now().toString();

			Map<String, AttributeValue> bodyMap = new HashMap<>();
			content.fields().forEachRemaining(entry ->
					bodyMap.put(entry.getKey(), new AttributeValue(entry.getValue().asText()))
			);

			Map<String, AttributeValue> item = new HashMap<>();
			item.put("id", new AttributeValue(eventId));
			item.put("principalId", new AttributeValue().withN(String.valueOf(principalId)));
			item.put("createdAt", new AttributeValue(createdAt));
			item.put("body", new AttributeValue().withM(bodyMap));

			saveToDynamoDb(item);

			Map<String, Object> responseBody = new HashMap<>();
			responseBody.put("id", eventId);
			responseBody.put("principalId", principalId);
			responseBody.put("createdAt", createdAt);
			responseBody.put("body", objectMapper.treeToValue(content, Map.class));

			return APIGatewayV2HTTPResponse.builder()
					.withStatusCode(201)
					.withBody(objectMapper.writeValueAsString(Map.of("statusCode", 201, "event", responseBody)))
					.build();
		} catch (Exception e) {
			context.getLogger().log("Error processing request: " + e.getMessage());
			return APIGatewayV2HTTPResponse.builder()
					.withStatusCode(500)
					.withBody("{\"statusCode\":500,\"message\":\"Fatal Error\"}")
					.build();
		}
	}

	private void saveToDynamoDb(Map<String, AttributeValue> itemValues) {
		AmazonDynamoDBClientBuilder.defaultClient().putItem(System.getenv("target_table"), itemValues);
	}
}
