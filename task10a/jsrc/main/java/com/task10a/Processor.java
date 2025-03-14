package com.task10a;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.google.gson.JsonObject;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.*;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
		lambdaName = "processor",
		roleName = "processor-role",
		layers = "weather-api-layer",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		tracingMode = TracingMode.Active,
		runtime = DeploymentRuntime.JAVA11,
		architecture = Architecture.ARM64,
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaLayer(
		layerName = "weather-api-layer",
		libraries = {"lib/sdk-1.10.0.jar"},
		runtime = DeploymentRuntime.JAVA11,
		architectures = {Architecture.ARM64},
		artifactExtension = ArtifactExtension.ZIP
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
@EnvironmentVariable(key = "target_table", value = "${target_table}")
public class Processor implements RequestHandler<APIGatewayV2HTTPEvent, Object> {

	public Object handleRequest(APIGatewayV2HTTPEvent request, Context context) {
		context.getLogger().log("Request received: " + request);
		App client = new App(50.4375, 30.5);
		try {
			JsonObject weatherData = client.getWeatherForecast();
			context.getLogger().log("Weather received: " + weatherData.toString());
			//persist.persistData(weatherData, System.getenv("target_table"), context.getLogger());
			return weatherData.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
