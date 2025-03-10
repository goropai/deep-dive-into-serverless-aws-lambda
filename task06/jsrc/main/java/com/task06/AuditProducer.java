package com.task06;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
    lambdaName = "audit_producer",
	roleName = "audit_producer-role",
	isPublishVersion = true,
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariable(
		key = "target_table", value = "${target_table}"
)
@DynamoDbTriggerEventSource(
		targetTable = "Configuration",
		batchSize = 1
)
@DependsOn(name = "Configuration", resourceType = ResourceType.DYNAMODB_TABLE)
public class AuditProducer implements RequestHandler<DynamodbEvent, Void> {
	private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

	@Override
	public Void handleRequest(DynamodbEvent event, Context context) {
		context.getLogger().log("DynamoDB event received: " + GSON.toJson(event));
		event.getRecords().forEach(rec -> processDynamoDBRecord(rec, context));
		return null;
	}

	private void processDynamoDBRecord(DynamodbEvent.DynamodbStreamRecord record, Context context) {
		context.getLogger().log("DynamoDB Record processed: " + GSON.toJson(record.getDynamodb()));

		Map<String, com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue> recordKeys
				= record.getDynamodb().getKeys();
		Map<String, com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue> newImage = record.getDynamodb().getNewImage();
		Map<String, com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue> oldImage = record.getDynamodb().getOldImage();
		String key = recordKeys.get("key").getS();
		Map<String, AttributeValue> item = buildAuditItem(key);
		if (oldImage != null) {
			item.put("updatedAttribute", new AttributeValue().withS("value"));
			item.put("oldValue", new AttributeValue().withN(oldImage.get("value").getN()));
			item.put("newValue", new AttributeValue().withN(newImage.get("value").getN()));
		}
		else {
			Map<String, AttributeValue> newValue = Map.of(
					"key", new AttributeValue().withS(key),
					"value", new AttributeValue().withN(newImage.get("value").getN()));
			item.put("newValue", new AttributeValue().withM(newValue));
		}
		putIntoAudit(item);
	}

	private Map<String, AttributeValue> buildAuditItem(String key) {
		Map<String, AttributeValue> item = new HashMap<>();
		item.put("id", new AttributeValue().withS(UUID.randomUUID().toString()));
		item.put("itemKey", new AttributeValue().withN(key));
		item.put("modificationTime", new AttributeValue().withS(DateTimeFormatter.ISO_INSTANT.format(Instant.now())));
		return item;
	}

	private static void putIntoAudit(Map<String, AttributeValue> update) {
		PutItemRequest putItemRequest = new PutItemRequest()
				.withTableName(System.getenv("target_table"))
				.withItem(update);
		AmazonDynamoDBClientBuilder.defaultClient().putItem(putItemRequest);
	}
}
