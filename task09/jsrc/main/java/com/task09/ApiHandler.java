package com.task09;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
    lambdaName = "api_handler",
	roleName = "api_handler-role",
	aliasName = "${lambdas_alias_name}",
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@LambdaUrlConfig(
		authType = AuthType.NONE,
		invokeMode = InvokeMode.BUFFERED
)
public class ApiHandler implements RequestHandler<APIGatewayV2HTTPEvent, String> {
	private LambdaLogger logger;
	private final ObjectMapper objectMapper = new ObjectMapper();
	private static final String URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";

	@Override
	public String handleRequest(APIGatewayV2HTTPEvent event, Context context) {
		logger = context.getLogger();
		String path = getPath(event);
		String method = getMethod(event);
		try {
			if ("/weather".equals(path) && "GET".equalsIgnoreCase(method)) {
				return executeGet(URL);
			}
			else {
				throw new RuntimeException("Bad request syntax or unsupported method");
			}
		} catch (Exception exception) {
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

	private String executeGet(String url) {
		try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
			HttpGet httpget = new HttpGet(url);

			logger.log("Executing request " + httpget.getRequestLine());

			ResponseHandler<String> responseHandler = response -> {
				int status = response.getStatusLine().getStatusCode();
				if (status >= 200 && status < 300) {
					HttpEntity entity = response.getEntity();
					return entity != null ? EntityUtils.toString(entity) : null;
				} else {
					throw new ClientProtocolException("Unexpected response status: " + status);
				}
			};
			return httpclient.execute(httpget, responseHandler);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private String getMethod(APIGatewayV2HTTPEvent requestEvent) {
		return requestEvent.getRequestContext().getHttp().getMethod();
	}

	private String getPath(APIGatewayV2HTTPEvent requestEvent) {
		return requestEvent.getRequestContext().getHttp().getPath();
	}
}
