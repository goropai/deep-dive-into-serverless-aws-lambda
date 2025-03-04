package com.task04;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.Iterator;
import java.util.List;

@LambdaHandler(
    lambdaName = "sns_handler",
	roleName = "sns_handler-role",
	isPublishVersion = true,
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
public class SnsHandler implements RequestHandler<SNSEvent, Boolean> {

	@Override
	public Boolean handleRequest(SNSEvent event, Context context) {
		List<SNSEvent.SNSRecord> records = event.getRecords();
		if (!records.isEmpty()) {
			Iterator<SNSEvent.SNSRecord> recordsIter = records.iterator();
			while (recordsIter.hasNext()) {
				processRecord(recordsIter.next(), context);
			}
		}
		return Boolean.TRUE;
	}

	public void processRecord(SNSEvent.SNSRecord record, Context context) {
		try {
			context.getLogger().log(record.getSNS().getMessage());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
