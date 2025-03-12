package com.task08;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.events.RuleEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.io.ByteArrayInputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RuleEventSource(
        targetRule = "uuid_trigger"
)
@LambdaHandler(
        lambdaName = "uuid_generator",
        roleName = "uuid_generator-role",
        aliasName = "${lambdas_alias_name}",
        logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@EnvironmentVariable(
        key = "BUCKET_NAME",
        value = "${target_bucket}"
)
public class UuidGenerator implements RequestHandler<Object, String> {
    private final String bucketName;
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    public UuidGenerator() {
        bucketName = System.getenv("BUCKET_NAME");
    }

    public String handleRequest(Object request, Context context) {
        final LambdaLogger logger = context.getLogger();
        final List<String> strings = IntStream.range(0, Math.max(1, 10))
                .mapToObj(i -> UUID.randomUUID().toString())
                .collect(Collectors.toList());
        Map<String, List<String>> uuids = Map.of("ids", strings);

        logger.log("UUIDs: " + uuids);

        final String objectName = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME);
        final String content = makeJson(uuids);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.getBytes().length);

        s3Client.putObject(bucketName, objectName, new ByteArrayInputStream(content.getBytes()), metadata);
        String result = "Object " + objectName + " was uploaded.";
        logger.log(result);
        return result;
    }

    private String makeJson(Map<String, List<String>> uuids) {
        try {
            return new ObjectMapper().writeValueAsString(uuids);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert to JSON", e);
        }
    }
}
