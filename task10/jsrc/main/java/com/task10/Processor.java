package com.task10;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaLayer;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.*;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.io.IOException;
import java.util.*;

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
			persistData(weatherData, System.getenv("target_table"), context.getLogger());
			return weatherData.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void persistData(JsonObject weatherData, String targetTable, LambdaLogger logger) {
		AmazonDynamoDB dynamoDB = AmazonDynamoDBClientBuilder.defaultClient();

		// 1. Create the top-level item structure
		Map<String, AttributeValue> item = new HashMap<>();
		String id = UUID.randomUUID().toString();
		item.put("id", new AttributeValue(id));

		// 2. Create the 'forecast' attribute (a Map)
		Map<String, AttributeValue> forecastMap = new HashMap<>();

		forecastMap.put("elevation", new AttributeValue().withN(weatherData.get("elevation").getAsString()));
		forecastMap.put("generationtime_ms", new AttributeValue().withN(weatherData.get("generationtime_ms").getAsString()));
		forecastMap.put("latitude", new AttributeValue().withN(weatherData.get("latitude").getAsString()));
		forecastMap.put("longitude", new AttributeValue().withN(weatherData.get("longitude").getAsString()));
		forecastMap.put("timezone", new AttributeValue(weatherData.get("timezone").getAsString()));
		forecastMap.put("timezone_abbreviation", new AttributeValue(weatherData.get("timezone_abbreviation").getAsString()));
		forecastMap.put("utc_offset_seconds", new AttributeValue().withN(weatherData.get("utc_offset_seconds").getAsString()));
		logger.log("Step 1: " + forecastMap);

		// 3. Create 'hourly' map
		Map<String, AttributeValue> hourlyMap = new HashMap<>();

		// 3.1 Extract "time" array
		JsonArray timeArray = weatherData.getAsJsonObject("hourly").getAsJsonArray("time");
		List<AttributeValue> timeList = new ArrayList<>();
		for (JsonElement time : timeArray) {
			timeList.add(new AttributeValue(time.getAsString()));
		}
		hourlyMap.put("time", new AttributeValue().withL(timeList));

		// 3.2 Extract "temperature_2m" array
		JsonArray temperatureArray = weatherData.getAsJsonObject("hourly").getAsJsonArray("temperature_2m");
		List<AttributeValue> temperatureList = new ArrayList<>();
		for (JsonElement temperature : temperatureArray) {
			temperatureList.add(new AttributeValue().withN(temperature.getAsString()));
		}
		hourlyMap.put("temperature_2m", new AttributeValue().withL(temperatureList));

		forecastMap.put("hourly", new AttributeValue().withM(hourlyMap));
		logger.log("Step 2: " + forecastMap);

		// 4. Create 'hourly_units' map
		Map<String, AttributeValue> hourlyUnitsMap = new HashMap<>();
		hourlyUnitsMap.put("time", new AttributeValue(weatherData.getAsJsonObject("hourly_units").get("time").getAsString()));
		hourlyUnitsMap.put("temperature_2m", new AttributeValue(weatherData.getAsJsonObject("hourly_units").get("temperature_2m").getAsString()));
		forecastMap.put("hourly_units", new AttributeValue().withM(hourlyUnitsMap));
		logger.log("Step 3: " + forecastMap);

		item.put("forecast", new AttributeValue().withM(forecastMap));

		// 5.  Create PutItemRequest
		PutItemRequest putItemRequest = new PutItemRequest()
				.withTableName(targetTable)
				.withItem(item);

		// 6. Save data in dynamoDB
		try {
			dynamoDB.putItem(putItemRequest);
			logger.log("Successfully persisted weather data with ID: " + id + " to table: " + targetTable);
		} catch (Exception e) {
			logger.log("Failed to persist weather data: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		String jsonString = "{\"latitude\":50.4375,\"longitude\":30.5,\"generationtime_ms\":0.0642538070678711,\"utc_offset_seconds\":0,\"timezone\":\"GMT\",\"timezone_abbreviation\":\"GMT\",\"elevation\":130.0,\"current_units\":{\"time\":\"iso8601\",\"interval\":\"seconds\",\"temperature_2m\":\"°C\",\"wind_speed_10m\":\"km/h\"},\"current\":{\"time\":\"2025-03-14T18:00\",\"interval\":900,\"temperature_2m\":11.4,\"wind_speed_10m\":0.7},\"hourly_units\":{\"time\":\"iso8601\",\"temperature_2m\":\"°C\",\"relative_humidity_2m\":\"%\",\"wind_speed_10m\":\"km/h\"},\"hourly\":{\"time\":[\"2025-03-14T00:00\",\"2025-03-14T01:00\",\"2025-03-14T02:00\",\"2025-03-14T03:00\",\"2025-03-14T04:00\",\"2025-03-14T05:00\",\"2025-03-14T06:00\",\"2025-03-14T07:00\",\"2025-03-14T08:00\",\"2025-03-14T09:00\",\"2025-03-14T10:00\",\"2025-03-14T11:00\",\"2025-03-14T12:00\",\"2025-03-14T13:00\",\"2025-03-14T14:00\",\"2025-03-14T15:00\",\"2025-03-14T16:00\",\"2025-03-14T17:00\",\"2025-03-14T18:00\",\"2025-03-14T19:00\",\"2025-03-14T20:00\",\"2025-03-14T21:00\",\"2025-03-14T22:00\",\"2025-03-14T23:00\",\"2025-03-15T00:00\",\"2025-03-15T01:00\",\"2025-03-15T02:00\",\"2025-03-15T03:00\",\"2025-03-15T04:00\",\"2025-03-15T05:00\",\"2025-03-15T06:00\",\"2025-03-15T07:00\",\"2025-03-15T08:00\",\"2025-03-15T09:00\",\"2025-03-15T10:00\",\"2025-03-15T11:00\",\"2025-03-15T12:00\",\"2025-03-15T13:00\",\"2025-03-15T14:00\",\"2025-03-15T15:00\",\"2025-03-15T16:00\",\"2025-03-15T17:00\",\"2025-03-15T18:00\",\"2025-03-15T19:00\",\"2025-03-15T20:00\",\"2025-03-15T21:00\",\"2025-03-15T22:00\",\"2025-03-15T23:00\",\"2025-03-16T00:00\",\"2025-03-16T01:00\",\"2025-03-16T02:00\",\"2025-03-16T03:00\",\"2025-03-16T04:00\",\"2025-03-16T05:00\",\"2025-03-16T06:00\",\"2025-03-16T07:00\",\"2025-03-16T08:00\",\"2025-03-16T09:00\",\"2025-03-16T10:00\",\"2025-03-16T11:00\",\"2025-03-16T12:00\",\"2025-03-16T13:00\",\"2025-03-16T14:00\",\"2025-03-16T15:00\",\"2025-03-16T16:00\",\"2025-03-16T17:00\",\"2025-03-16T18:00\",\"2025-03-16T19:00\",\"2025-03-16T20:00\",\"2025-03-16T21:00\",\"2025-03-16T22:00\",\"2025-03-16T23:00\",\"2025-03-17T00:00\",\"2025-03-17T01:00\",\"2025-03-17T02:00\",\"2025-03-17T03:00\",\"2025-03-17T04:00\",\"2025-03-17T05:00\",\"2025-03-17T06:00\",\"2025-03-17T07:00\",\"2025-03-17T08:00\",\"2025-03-17T09:00\",\"2025-03-17T10:00\",\"2025-03-17T11:00\",\"2025-03-17T12:00\",\"2025-03-17T13:00\",\"2025-03-17T14:00\",\"2025-03-17T15:00\",\"2025-03-17T16:00\",\"2025-03-17T17:00\",\"2025-03-17T18:00\",\"2025-03-17T19:00\",\"2025-03-17T20:00\",\"2025-03-17T21:00\",\"2025-03-17T22:00\",\"2025-03-17T23:00\",\"2025-03-18T00:00\",\"2025-03-18T01:00\",\"2025-03-18T02:00\",\"2025-03-18T03:00\",\"2025-03-18T04:00\",\"2025-03-18T05:00\",\"2025-03-18T06:00\",\"2025-03-18T07:00\",\"2025-03-18T08:00\",\"2025-03-18T09:00\",\"2025-03-18T10:00\",\"2025-03-18T11:00\",\"2025-03-18T12:00\",\"2025-03-18T13:00\",\"2025-03-18T14:00\",\"2025-03-18T15:00\",\"2025-03-18T16:00\",\"2025-03-18T17:00\",\"2025-03-18T18:00\",\"2025-03-18T19:00\",\"2025-03-18T20:00\",\"2025-03-18T21:00\",\"2025-03-18T22:00\",\"2025-03-18T23:00\",\"2025-03-19T00:00\",\"2025-03-19T01:00\",\"2025-03-19T02:00\",\"2025-03-19T03:00\",\"2025-03-19T04:00\",\"2025-03-19T05:00\",\"2025-03-19T06:00\",\"2025-03-19T07:00\",\"2025-03-19T08:00\",\"2025-03-19T09:00\",\"2025-03-19T10:00\",\"2025-03-19T11:00\",\"2025-03-19T12:00\",\"2025-03-19T13:00\",\"2025-03-19T14:00\",\"2025-03-19T15:00\",\"2025-03-19T16:00\",\"2025-03-19T17:00\",\"2025-03-19T18:00\",\"2025-03-19T19:00\",\"2025-03-19T20:00\",\"2025-03-19T21:00\",\"2025-03-19T22:00\",\"2025-03-19T23:00\",\"2025-03-20T00:00\",\"2025-03-20T01:00\",\"2025-03-20T02:00\",\"2025-03-20T03:00\",\"2025-03-20T04:00\",\"2025-03-20T05:00\",\"2025-03-20T06:00\",\"2025-03-20T07:00\",\"2025-03-20T08:00\",\"2025-03-20T09:00\",\"2025-03-20T10:00\",\"2025-03-20T11:00\",\"2025-03-20T12:00\",\"2025-03-20T13:00\",\"2025-03-20T14:00\",\"2025-03-20T15:00\",\"2025-03-20T16:00\",\"2025-03-20T17:00\",\"2025-03-20T18:00\",\"2025-03-20T19:00\",\"2025-03-20T20:00\",\"2025-03-20T21:00\",\"2025-03-20T22:00\",\"2025-03-20T23:00\"],\"temperature_2m\":[11.8,11.6,11.6,11.5,11.0,10.7,11.3,12.0,12.2,12.8,13.0,13.7,14.9,15.1,15.0,14.5,13.4,12.1,11.3,10.6,10.2,9.5,9.2,8.6,8.5,8.5,8.5,8.6,9.0,9.5,10.0,9.8,7.2,5.9,4.7,4.0,3.8,3.8,3.8,3.6,3.0,2.5,1.8,1.6,1.3,1.1,1.6,1.4,1.2,0.9,0.7,0.6,0.5,0.3,0.5,1.1,2.3,3.7,4.9,6.1,6.9,6.9,6.8,6.4,5.9,5.3,4.9,4.4,3.7,3.1,2.5,1.8,1.1,0.7,0.2,-0.0,-0.4,-0.6,0.0,1.7,2.8,3.2,2.1,1.7,1.8,2.5,2.3,1.7,0.9,0.5,0.0,-0.6,-1.3,-1.9,-2.4,-2.8,-3.3,-3.9,-4.6,-4.9,-5.0,-4.8,-4.2,-3.1,-1.6,-0.2,1.0,2.1,2.9,3.3,3.4,3.2,2.6,1.7,1.0,0.5,0.2,0.0,-0.1,-0.1,-0.2,-0.3,-0.4,-0.4,-0.5,-0.6,-0.3,0.6,1.8,3.1,4.4,5.9,6.9,7.2,7.3,6.9,5.8,4.2,2.9,2.1,1.6,1.1,0.6,0.1,-0.3,-0.7,-1.1,-1.2,-1.1,-0.7,0.2,1.9,4.2,6.2,7.7,8.9,9.6,9.8,9.5,8.8,7.5,5.8,4.3,3.5,2.9,2.5,2.0,1.7],\"relative_humidity_2m\":[82,78,71,67,69,70,68,64,61,53,47,44,41,41,41,42,48,54,56,59,62,66,68,74,75,75,77,78,77,74,73,76,81,80,80,81,81,79,78,77,78,81,85,85,86,85,74,75,77,78,79,79,80,80,79,76,70,65,61,56,54,53,53,54,55,57,55,54,53,53,56,59,58,60,63,65,68,69,66,59,58,62,74,70,57,46,39,36,35,36,41,42,43,44,46,50,53,57,62,65,66,66,63,56,47,39,34,31,29,28,29,31,35,41,47,51,55,59,63,66,70,74,78,82,86,88,88,83,74,65,55,44,36,33,29,27,30,36,41,45,49,52,55,57,59,60,62,62,62,62,60,55,48,42,37,33,30,30,32,35,40,45,52,60,69,76,80,81],\"wind_speed_10m\":[10.2,11.0,12.8,13.2,13.5,13.5,15.7,18.7,18.3,16.6,14.4,12.7,12.7,12.3,12.7,9.7,4.5,2.3,0.7,2.2,2.1,4.0,4.4,6.5,6.8,7.4,8.4,8.0,6.9,7.8,7.6,9.2,11.5,13.1,13.0,13.8,14.1,14.0,13.0,11.5,11.2,9.8,9.5,10.4,9.7,9.9,10.3,9.8,8.4,9.0,7.7,7.6,8.6,7.1,7.9,7.9,7.8,7.9,7.8,9.2,10.3,10.1,9.2,9.0,7.2,6.8,6.5,7.6,9.0,8.6,7.9,7.6,6.9,7.4,7.8,7.9,8.4,7.8,8.4,8.4,10.3,12.5,15.4,16.4,16.4,17.1,17.1,16.7,14.1,12.6,12.6,12.6,13.4,13.6,13.2,12.7,12.4,12.0,11.9,11.6,11.1,10.6,10.4,10.9,12.0,12.9,12.9,12.9,12.3,11.9,11.5,10.7,9.4,7.8,7.1,7.1,7.7,8.4,8.8,9.2,9.3,9.3,9.2,9.2,8.8,8.9,9.0,10.0,11.3,12.2,12.6,12.7,12.6,14.1,14.1,13.6,11.4,9.0,7.6,7.6,8.4,9.0,9.4,9.7,10.1,10.1,9.7,9.7,10.1,10.5,10.9,11.5,12.2,13.0,13.9,15.3,15.7,15.0,13.7,12.0,9.9,7.9,7.1,6.2,5.2,4.5,4.2,4.0]}}";

		Gson gson = new Gson();
		JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);

		new Processor().persistData(jsonObject, "Weather", new LambdaLogger() {
			@Override
			public void log(String message) {
				System.out.println(message);
			}

			@Override
			public void log(byte[] message) {
				System.out.println(new String(message));
			}
		});
	}
}
