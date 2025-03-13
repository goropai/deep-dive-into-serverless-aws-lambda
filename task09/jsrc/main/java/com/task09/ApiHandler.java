package com.task09;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.Architecture;
import com.syndicate.deployment.model.ArtifactExtension;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.TracingMode;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
		lambdaName = "api_handler",
		roleName = "api_handler-role",
		layers = {"open-meteo-api-layer"},
		architecture = Architecture.ARM64,
		tracingMode = TracingMode.Active,
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaLayer(
		layerName = "open-meteo-api-layer",
		libraries = {"lib/httpclient-4.5.13.jar"},
		architectures = {Architecture.ARM64},
		artifactExtension = ArtifactExtension.ZIP
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
public class ApiHandler implements RequestHandler<APIGatewayV2HTTPEvent, String> {
	private final ObjectMapper objectMapper = new ObjectMapper();
	private static final String URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";

	@Override
	public String handleRequest(APIGatewayV2HTTPEvent event, Context context) {
		String path = getPath(event);
		String method = getMethod(event);
		try {
			if ("/weather".equals(path) && "GET".equalsIgnoreCase(method)) {
				return HttpClient.executeGet(URL);
			}
			else {
				throw new RuntimeException("Bad request syntax or unsupported method");
			}
		}
		catch (Exception exception) {
			Map<String, Object> resultMap = new HashMap<>();
			resultMap.put("statusCode", 400);
			resultMap.put("message", String.format("Bad request syntax or unsupported method. Request path: %s. HTTP method: %s", path, method));
			try {
				return objectMapper.writeValueAsString(resultMap);
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private String getMethod(APIGatewayV2HTTPEvent requestEvent) {
		return requestEvent.getRequestContext().getHttp().getMethod();
	}

	private String getPath(APIGatewayV2HTTPEvent requestEvent) {
		return requestEvent.getRequestContext().getHttp().getPath();
	}
}
