package com.task02;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.Architecture;
import com.syndicate.deployment.model.ArtifactExtension;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@LambdaHandler(
    lambdaName = "hello_world",
	roleName = "hello_world-role",
	isPublishVersion = true,
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
/*@LambdaLayer(
		layerName = "sdk-layer",
		runtime = DeploymentRuntime.JAVA17,
		architectures = {Architecture.ARM64},
		artifactExtension = ArtifactExtension.ZIP
)*/
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
public class HelloWorld implements RequestHandler<APIGatewayV2HTTPEvent, Map<String, Object>> {

	private static final int SC_OK = 200;
	private static final int SC_NOT_FOUND = 400;
	private final Map<String, String> responseHeaders = Map.of("Content-Type", "application/json");
	private final Map<String, Function<APIGatewayV2HTTPEvent, Map<String, Object>>> routeHandlers = Map.of(
			"GET#/hello", this::handleGetHello
	);

	@Override
	public Map<String, Object> handleRequest(APIGatewayV2HTTPEvent requestEvent, Context context) {
		String key = getMethod(requestEvent) + "#" + getPath(requestEvent);
		return routeHandlers.getOrDefault(key, this::notFoundResponse).apply(requestEvent);
	}

	private Map<String, Object> handleGetHello(APIGatewayV2HTTPEvent requestEvent) {
		return buildResponse(SC_OK, "Hello from Lambda");
	}

	private Map<String, Object> notFoundResponse(APIGatewayV2HTTPEvent requestEvent) {
		return buildResponse(SC_NOT_FOUND,
				"Bad request syntax or unsupported method. Request path: %s. HTTP method: %s".formatted(
				getMethod(requestEvent),
				getPath(requestEvent)
		));
	}

	private Map<String, Object> buildResponse(int statusCode, Object message) {
		Map<String, Object> resultMap = new HashMap<>();
		resultMap.put("statusCode", statusCode);
		resultMap.put("body", "{\"statusCode\":" + statusCode + ", \"message\": \"" + message + "\"}");
		return resultMap;
	}

	private String getMethod(APIGatewayV2HTTPEvent requestEvent) {
		return requestEvent.getRequestContext().getHttp().getMethod();
	}

	private String getPath(APIGatewayV2HTTPEvent requestEvent) {
		return requestEvent.getRequestContext().getHttp().getPath();
	}
}
