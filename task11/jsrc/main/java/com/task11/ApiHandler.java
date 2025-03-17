package com.task11;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;
import com.task11.utils.CognitoHelper;
import com.task11.utils.EmailValidator;
import com.task11.utils.PasswordValidator;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.time.LocalTime;
import java.util.*;

import static com.syndicate.deployment.model.environment.ValueTransformer.USER_POOL_NAME_TO_CLIENT_ID;
import static com.syndicate.deployment.model.environment.ValueTransformer.USER_POOL_NAME_TO_USER_POOL_ID;

@LambdaHandler(
    lambdaName = "api_handler",
	roleName = "api_handler-role",
	isPublishVersion = true,
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
@DependsOn(name = "${tables_table}", resourceType = ResourceType.DYNAMODB_TABLE)
@DependsOn(name = "${reservations_table}", resourceType = ResourceType.DYNAMODB_TABLE)
@DependsOn(resourceType = ResourceType.COGNITO_USER_POOL, name = "${booking_userpool}")
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "tables_table", value = "${tables_table}"),
		@EnvironmentVariable(key = "reservations_table", value = "${reservations_table}"),
		@EnvironmentVariable(key = "booking_userpool", value = "${booking_userpool}"),
		@EnvironmentVariable(key = "REGION", value = "${region}"),
		@EnvironmentVariable(key = "COGNITO_ID", value = "${pool_name}", valueTransformer = USER_POOL_NAME_TO_USER_POOL_ID),
		@EnvironmentVariable(key = "CLIENT_ID", value = "${pool_name}", valueTransformer = USER_POOL_NAME_TO_CLIENT_ID)
})
public class ApiHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
	private final CognitoIdentityProviderClient cognitoClient = CognitoIdentityProviderClient.create();
	private LambdaLogger logger = null;

	public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
		logger = context.getLogger();
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, String> request;
		try {
			request = objectMapper.readValue(objectMapper.writeValueAsString(event), LinkedHashMap.class);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}

		String path = (String) request.get("path");
		String method = (String) request.get("httpMethod");


		logger.log("Path: " + path + " method: " + method);
		try {
			logger.log("Request: " + objectMapper.writeValueAsString(event));
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}


		if ("/signup".equals(path) && "POST".equalsIgnoreCase(method)) {
			return signUp(event);
		}

		if ("/signin".equals(path) && "POST".equalsIgnoreCase(method)) {
			return signIn(event);
		}

		if("/tables".equals(path) && "POST".equalsIgnoreCase(method)){
			return postTable(event);
		}

		if("/tables".equals(path) && "GET".equalsIgnoreCase(method)){
			return getTables();
		}

		Map<String, Object> pathParameters = null;
		try {
			pathParameters = objectMapper.readValue(objectMapper.writeValueAsString(event.getPathParameters()), LinkedHashMap.class);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
		if ("GET".equalsIgnoreCase(method) && pathParameters != null) {
			if (pathParameters.containsKey("tableId")) {
				try {
					return getTableById(objectMapper.writeValueAsString(event.getPathParameters().get("tableId")));
				} catch (JsonProcessingException e) {
					throw new RuntimeException(e);
				}
			}
		}

		if("/reservations".equals(path) && "POST".equalsIgnoreCase(method)){
			return postReservation(event);
		}

		if("/reservations".equals(path) && "GET".equalsIgnoreCase(method)){
			return getReservations();
		}

		return new APIGatewayProxyResponseEvent();
	}

	private APIGatewayProxyResponseEvent signUp(APIGatewayProxyRequestEvent event) {
		APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			Map<String, Object> body = objectMapper.readValue(event.getBody(), Map.class);
			logger.log("signUp was called");
			String email = String.valueOf(body.get("email"));
			String password = String.valueOf(body.get("password"));

			if (!EmailValidator.validateEmail(email)) {
				logger.log("Email is invalid");
				throw new Exception("Email is invalid");
			}

			if (!PasswordValidator.validatePassword(password)) {
				logger.log("Password is invalid");
				throw new Exception("Email is invalid");
			}

			String userPoolId = new CognitoHelper().getUserPoolIdByName(System.getenv("bookingUserPool"))
					.orElseThrow(() -> new IllegalArgumentException("No such user pool"));

			AdminCreateUserRequest adminCreateUserRequest = AdminCreateUserRequest
					.builder()
					.userPoolId(userPoolId)
					.username(email)
					.userAttributes(AttributeType.builder().name("email").value(email).build())
					.messageAction(MessageActionType.SUPPRESS)
					.build();
			logger.log(adminCreateUserRequest.toString());
			AdminSetUserPasswordRequest adminSetUserPassword = AdminSetUserPasswordRequest
					.builder()
					.password(password)
					.userPoolId(userPoolId)
					.username(email)
					.permanent(true)
					.build();
			logger.log(adminSetUserPassword.toString());

			cognitoClient.adminCreateUser(adminCreateUserRequest);
			cognitoClient.adminSetUserPassword(adminSetUserPassword);

			response.setStatusCode(200);

		} catch (Exception ex) {
			logger.log(String.valueOf(ex));
			response.setStatusCode(400);
			response.setBody(ex.toString());
		}
		return response;
	}

	private APIGatewayProxyResponseEvent signIn(APIGatewayProxyRequestEvent event) {
		logger.log("signIn was called");
		APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
		ObjectMapper objectMapper = new ObjectMapper();

		try {

			Map<String, Object> body = objectMapper.readValue(event.getBody(), Map.class);
			logger.log("signUp was called");
			String email = String.valueOf(body.get("email"));
			String password = String.valueOf(body.get("password"));

			if (!EmailValidator.validateEmail(email)) {
				logger.log("Email is invalid");
				throw new Exception("Email is invalid");
			}

			if (!PasswordValidator.validatePassword(password)) {
				logger.log("Password is invalid");
				throw new Exception("Email is invalid");
			}

			String userPoolId = new CognitoHelper().getUserPoolIdByName(System.getenv("bookingUserPool"))
					.orElseThrow(() -> new IllegalArgumentException("No such user pool"));

			String clientId = new CognitoHelper()
					.getClientIdByUserPoolName(System.getenv("bookingUserPool"))
					.orElseThrow(() -> new IllegalArgumentException("No such client id"));

			Map<String, String> authParams = new HashMap<>();
			authParams.put("USERNAME", email);
			authParams.put("PASSWORD", password);
			logger.log(authParams.toString());
			AdminInitiateAuthRequest authRequest = AdminInitiateAuthRequest.builder()
					.authFlow(AuthFlowType.ADMIN_NO_SRP_AUTH)
					.userPoolId(userPoolId)
					.clientId(clientId)
					.authParameters(authParams)
					.build();
			logger.log(String.valueOf(authRequest));

			AdminInitiateAuthResponse result = cognitoClient.adminInitiateAuth(authRequest);
			String accessToken = result.authenticationResult().idToken();
			logger.log(accessToken);

			Map<String, Object> jsonResponse = new HashMap<>();
			jsonResponse.put("accessToken", accessToken);

			response.setStatusCode(200);
			response.setBody(objectMapper.writeValueAsString(jsonResponse));
			logger.log(objectMapper.writeValueAsString(jsonResponse));
		} catch (Exception ex) {
			logger.log(String.valueOf(ex));
			response.setStatusCode(400);
			response.setBody(ex.toString());
		}
		return response;
	}


	private APIGatewayProxyResponseEvent postTable(APIGatewayProxyRequestEvent event) {
		logger.log("postTable was called");
		APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
		ObjectMapper objectMapper = new ObjectMapper();
		AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard().build();
		try {


			Map<String, Object> body = objectMapper.readValue(event.getBody(), Map.class);
			logger.log(body.toString());
			String id = String.valueOf(body.get("id"));
			int number = (Integer) body.get("number");
			int places = (Integer) body.get("places");
			boolean isVip = (Boolean) body.get("isVip");
			int minOrder = -1;
			if (body.containsKey("minOrder")) {
				minOrder = (Integer) body.get("minOrder");
			}

			Item item = new Item()
					.withString("id", id)
					.withInt("number", number)
					.withInt("places", places)
					.withBoolean("isVip", isVip);
			if (minOrder != -1) {
				item.withInt("minOrder", minOrder);
			}
			logger.log(String.valueOf(item));
			ddb.putItem(System.getenv("tablesTable"), ItemUtils.toAttributeValues(item));


			Map<String, Object> jsonResponse = new HashMap<>();
			jsonResponse.put("id", Integer.parseInt(id));
			logger.log(jsonResponse.toString());
			response.setStatusCode(200);
			response.setBody(objectMapper.writeValueAsString(jsonResponse));
		} catch (
				Exception ex) {
			logger.log(String.valueOf(ex));
			response.setStatusCode(400);
			response.setBody(ex.toString());
		}
		return response;
	}

	private APIGatewayProxyResponseEvent getTables() {
		logger.log("getTables was called");
		APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
					.withRegion(System.getenv("region"))
					.build();

			ScanRequest scanRequest = new ScanRequest().withTableName(System.getenv("tablesTable"));
			ScanResult scanResult = ddb.scan(scanRequest);
			logger.log(String.valueOf(scanResult));

			List<Map<String, Object>> tables = new ArrayList<>();
			for (Map<String, AttributeValue> item : scanResult.getItems()) {
				Map<String, Object> table = new LinkedHashMap<>();
				table.put("id", Integer.parseInt(item.get("id").getS()));
				table.put("number", Integer.parseInt(item.get("number").getN()));
				table.put("places", Integer.parseInt(item.get("places").getN()));
				table.put("isVip", Boolean.parseBoolean(item.get("isVip").getBOOL().toString()));
				table.put("minOrder", Integer.parseInt(item.get("minOrder").getN()));
				tables.add(table);
			}
			logger.log(tables.toString());

			tables.sort(Comparator.comparing(o -> (Integer) o.get("id")));
			Map<String, Object> jsonResponse = new HashMap<>();
			jsonResponse.put("tables", tables);
			logger.log(jsonResponse.toString());
			response.setStatusCode(200);
			response.setBody(objectMapper.writeValueAsString(jsonResponse));
		} catch (Exception e) {
			response.setStatusCode(400);
		}
		return response;
	}


	private APIGatewayProxyResponseEvent getTableById(String tableId) {
		logger.log("getTableById was called");
		APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
					.withRegion(System.getenv("region"))
					.build();

			ScanRequest scanRequest = new ScanRequest().withTableName(System.getenv("tablesTable"));
			ScanResult scanResult = ddb.scan(scanRequest);
			logger.log(String.valueOf(scanResult));
			Map<String, AttributeValue> table = new HashMap<>();
			for (Map<String, AttributeValue> item : scanResult.getItems()) {
				int existingId = Integer.parseInt(item.get("id").getS().trim().replaceAll("\"", ""));
				int requiredId = Integer.parseInt(tableId.trim().replaceAll("\"", ""));
				if (existingId == requiredId) {
					table = item;
				}
			}
			logger.log(table.toString());
			Map<String, Object> jsonResponse = ItemUtils.toSimpleMapValue(table);
			jsonResponse.replace("id", Integer.parseInt((String) jsonResponse.get("id")));

			logger.log(jsonResponse.toString());
			response.setStatusCode(200);
			response.setBody(objectMapper.writeValueAsString(jsonResponse));
		} catch (Exception e) {
			response.setStatusCode(400);
		}
		return response;
	}

	private APIGatewayProxyResponseEvent postReservation(APIGatewayProxyRequestEvent event){
		logger.log("postReservation was called");
		APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
		ObjectMapper objectMapper = new ObjectMapper();
		AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
				.withRegion(System.getenv("region"))
				.build();
		try{
			Map<String, Object> body = objectMapper.readValue(event.getBody(), Map.class);
			logger.log(body.toString());

			String reservationId = UUID.randomUUID().toString();
			String tableNumber = String.valueOf(body.get("tableNumber"));
			String clientName = String.valueOf(body.get("clientName"));
			String phoneNumber = String.valueOf(body.get("phoneNumber"));
			String date = String.valueOf(body.get("date"));
			String slotTimeStart = String.valueOf(body.get("slotTimeStart"));
			String slotTimeEnd = String.valueOf(body.get("slotTimeEnd"));


			Item item = new Item()
					.withString("id", reservationId)
					.withString("tableNumber", tableNumber)
					.withString("clientName", clientName)
					.withString("phoneNumber", phoneNumber)
					.withString("date", date)
					.withString("slotTimeStart", slotTimeStart)
					.withString("slotTimeEnd", slotTimeEnd);



			logger.log(String.valueOf(item));

			if (!tableExists(ddb,System.getenv("tablesTable"), tableNumber)) {
				response.setStatusCode(400);
				response.setBody("Table does not exist");
				logger.log("Table does not exist");
				return response;
			}

			if (isOverlappingReservation(ddb,System.getenv("reservationsTable"), tableNumber, date, slotTimeStart, slotTimeEnd)) {
				response.setStatusCode(400);
				response.setBody("Reservation overlaps with an existing reservation");
				logger.log("Reservation overlaps with an existing reservation");
				return response;
			}


			ddb.putItem(System.getenv("reservationsTable"), ItemUtils.toAttributeValues(item));

			Map<String, Object> jsonResponse = new HashMap<>();
			jsonResponse.put("reservationId", reservationId);
			logger.log(jsonResponse.toString());
			response.setStatusCode(200);
			response.setBody(objectMapper.writeValueAsString(jsonResponse));
		} catch (Exception ex){
			response.setStatusCode(400);
		}
		return response;
	}

	private APIGatewayProxyResponseEvent getReservations() {
		logger.log("getReservations was called");
		APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
					.withRegion(System.getenv("region"))
					.build();

			ScanRequest scanRequest = new ScanRequest().withTableName(System.getenv("reservationsTable"));
			ScanResult scanResult = ddb.scan(scanRequest);
			logger.log(String.valueOf(scanResult));

			List<Map<String, Object>> reservations = new ArrayList<>();
			for (Map<String, AttributeValue> item : scanResult.getItems()) {
				item.remove("id");
				Map<String, Object> reservation = new LinkedHashMap<>();
				reservation.put("tableNumber", Integer.parseInt(item.get("tableNumber").getS()));
				reservation.put("clientName", item.get("clientName").getS());
				reservation.put("phoneNumber", item.get("phoneNumber").getS());
				reservation.put("date", item.get("date").getS());
				reservation.put("slotTimeStart", item.get("slotTimeStart").getS());
				reservation.put("slotTimeEnd", item.get("slotTimeEnd").getS());

				reservations.add(reservation);
			}

			logger.log(reservations.toString());

			Map<String, Object> jsonResponse = new HashMap<>();
			jsonResponse.put("reservations", reservations);
			logger.log(jsonResponse.toString());

			response.setStatusCode(200);
			response.setBody(objectMapper.writeValueAsString(jsonResponse));

		} catch (Exception ex) {
			response.setStatusCode(400);
		}
		return response;
	}

	public boolean tableExists(AmazonDynamoDB ddb, String tableName, String tableNumber) {
		ScanResult scanResult = ddb.scan(new ScanRequest().withTableName(tableName));

		for (Map<String, AttributeValue> item : scanResult.getItems()) {
			if (tableNumber.equals(item.get("number").getN())) {
				logger.log("Table exists, number: " + tableNumber);
				return true;
			}
		}
		return false;
	}

	public boolean isOverlappingReservation(AmazonDynamoDB ddb,String tableName, String tableNumber, String date, String slotTimeStart, String slotTimeEnd) {
		ScanResult scanResult = ddb.scan(new ScanRequest().withTableName(tableName));
		for (Map<String, AttributeValue> item : scanResult.getItems()) {
			String existingTableNumber = item.get("tableNumber").getS();
			String existingDate = item.get("date").getS();

			if (tableNumber.equals(existingTableNumber) && date.equals(existingDate)) {
				String existingSlotTimeStart = item.get("slotTimeStart").getS();
				String existingSlotTimeEnd = item.get("slotTimeEnd").getS();

				return isTimeOverlap(slotTimeStart, slotTimeEnd, existingSlotTimeStart, existingSlotTimeEnd);
			}
		}

		return false;
	}

	private boolean isTimeOverlap(String slotTimeStart, String slotTimeEnd, String existingSlotTimeStart, String existingSlotTimeEnd) {

		LocalTime start = LocalTime.parse(slotTimeStart);
		LocalTime end = LocalTime.parse(slotTimeEnd);
		LocalTime existingStart = LocalTime.parse(existingSlotTimeStart);
		LocalTime existingEnd = LocalTime.parse(existingSlotTimeEnd);

		return (start.isBefore(existingEnd) && end.isAfter(existingStart));
	}
}
