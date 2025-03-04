package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.syndicate.deployment.annotations.events.SqsTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

@LambdaHandler(
    lambdaName = "sqs_handler",
	roleName = "sqs_handler-role",
	isPublishVersion = true,
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@SqsTriggerEventSource(targetQueue = "async_queue", batchSize = 1)
public class SqsHandler implements RequestHandler<SQSEvent, Void> {

	@Override
	public Void handleRequest(SQSEvent sqsEvent, Context context) {
		for (SQSEvent.SQSMessage msg : sqsEvent.getRecords()) {
			processMessage(msg, context);
		}
		return null;
	}

	private void processMessage(SQSEvent.SQSMessage msg, Context context) {
		try {
			context.getLogger().log(msg.getBody());

			// TODO: Do interesting work based on the new message

		} catch (Exception e) {
			context.getLogger().log("An error occurred");
			throw e;
		}

	}
}
