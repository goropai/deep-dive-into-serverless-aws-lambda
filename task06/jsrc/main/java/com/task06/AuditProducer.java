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
		String updatedAt = Instant.now().toString();
		if (recordKeys.containsKey("id")) {
			String idValue = recordKeys.get("id").getS();
			GetItemRequest request = new GetItemRequest()
					.withTableName(System.getenv("target_table"))
					.withKey(Map.of("id", new AttributeValue(idValue)));
			Map<String, AttributeValue> existingItem = AmazonDynamoDBClientBuilder.defaultClient().getItem(request).getItem();
			context.getLogger().log("Existing item: " + GSON.toJson(existingItem));

			Map<String, AttributeValue> update = new HashMap<>();
			update.put("id", new AttributeValue(UUID.randomUUID().toString()));
			update.put("itemKey", new AttributeValue(record.getEventID()));
			update.put("modificationTime", new AttributeValue(updatedAt));
			update.put("updatedAttribute", new AttributeValue("value"));
			update.put("oldValue", new AttributeValue().withN(existingItem.get("newValue").getM().get("value").getN()));
			update.put("newValue", new AttributeValue().withN(record.getDynamodb().getNewImage().get("value").getN()));

			putIntoAudit(update);
		}
		else {
			Map<String, AttributeValue> item = new HashMap<>();
			item.put("id", new AttributeValue(UUID.randomUUID().toString()));
			item.put("itemKey", new AttributeValue().withN(String.valueOf(record.getEventID())));
			item.put("modificationTime", new AttributeValue(updatedAt));
			item.put("newValue", new AttributeValue().withS(GSON.toJson(record.getDynamodb())));

			putIntoAudit(item);
		}
	}

	private static void putIntoAudit(Map<String, AttributeValue> update) {
		PutItemRequest putItemRequest = new PutItemRequest()
				.withTableName("Audit")
				.withItem(update);
		AmazonDynamoDBClientBuilder.defaultClient().putItem(putItemRequest);
	}
}
