package com.task09;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.Architecture;
import com.syndicate.deployment.model.ArtifactExtension;
import com.syndicate.deployment.model.DeploymentRuntime;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.io.IOException;
import java.util.Map;

@LambdaHandler(
		lambdaName = "api_handler",
		roleName = "api_handler-role",
		layers = "api_handler-layer",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		runtime = DeploymentRuntime.JAVA11,
		architecture = Architecture.ARM64,
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaLayer(
		layerName = "api_handler-layer",
		libraries = {"lib/sdk-1.10.0.jar"},
		runtime = DeploymentRuntime.JAVA11,
		architectures = {Architecture.ARM64},
		artifactExtension = ArtifactExtension.ZIP
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
public class ApiHandler implements RequestHandler<APIGatewayV2HTTPEvent, Object> {

	public Object handleRequest(APIGatewayV2HTTPEvent request, Context context) {
		context.getLogger().log("Request received: " + request);
		String httpMethod = request.getRequestContext().getHttp().getMethod();
		String path = request.getRequestContext().getHttp().getPath();
		if ("GET".equalsIgnoreCase(httpMethod) && "/weather".equals(path)) {

			App client = new App(52.52, 13.41);
			try {
				JsonObject weatherData = client.getWeatherForecast();
				context.getLogger().log("Weather received: " + weatherData.toString());
				return weatherData.toString();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		else {
			String errorMessage = String.format("Bad request syntax or unsupported method. " +
					"Request path: %s. HTTP method: %s", path, httpMethod);
			return new Gson().toJson(Map.of("statusCode", 400, "message", errorMessage));
		}
	}
}
