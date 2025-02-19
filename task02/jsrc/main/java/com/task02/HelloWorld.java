package com.task02;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.util.Map;

@LambdaHandler(
		lambdaName = "hello_world",
		roleName = "hello_world-role",
		isPublishVersion = true,
		aliasName = /*"${lambdas_alias_name}"*/"learn",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
public class HelloWorld implements RequestHandler<APIGatewayV2HTTPEvent, APIGatewayV2HTTPResponse> {

	@Override
	public APIGatewayV2HTTPResponse handleRequest(APIGatewayV2HTTPEvent request, Context context) {
		String path = request.getRequestContext().getHttp().getPath();
		String method = request.getRequestContext().getHttp().getMethod();

		int statusCode;
		String responseBody;

		if ("/hello".equals(path) && "GET".equalsIgnoreCase(method)) {
			statusCode = 200;
			responseBody = "{\"statusCode\": 200, \"message\": \"Hello from Lambda\"}";
		} else {
			statusCode = 400;
			responseBody = String.format(
					"{\"statusCode\": 400, \"message\": \"Bad request syntax or unsupported method. Request path: %s. HTTP method: %s\"}",
					path, method
			);
		}

		return APIGatewayV2HTTPResponse.builder()
				.withStatusCode(statusCode)
				.withHeaders(Map.of("Content-Type", "application/json"))
				.withBody(responseBody)
				.build();
	}
}
