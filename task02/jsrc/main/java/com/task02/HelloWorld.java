package com.task02;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
    lambdaName = "hello_world",
	roleName = "hello_world-role",
	isPublishVersion = true,
	aliasName = /*"${lambdas_alias_name}"*/"learn",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
public class HelloWorld implements RequestHandler<Object, Map<String, Object>> {

	public Map<String, Object> handleRequest(Object request, Context context) {
		if (request instanceof APIGatewayProxyRequestEvent) {
			APIGatewayProxyRequestEvent event = (APIGatewayProxyRequestEvent) request;
			String path = event.getPath();
			String method = event.getHttpMethod();
			if ("/hello".equalsIgnoreCase(path) && "GET".equalsIgnoreCase(method)) {
				System.out.println("Hello from lambda");
				return generateResponse(200, "Hello from Lambda");
			} else {
				String errorMessage =
                        "Bad request syntax or unsupported method. Request path: " + path + ". HTTP method: " + method;
				return generateResponse(400, errorMessage);
			}
		}
		return generateResponse(400, "Hello from Lambda");
	}

	private Map<String, Object> generateResponse(int statusCode, String message) {
		Map<String, Object> resultMap = new HashMap<String, Object>();
		resultMap.put("statusCode", statusCode);
		resultMap.put("message", message);
		return resultMap;
	}
}
