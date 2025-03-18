package com.task12c.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.task12c.dto.Table;
import com.task12c.dto.Tables;
import lombok.SneakyThrows;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class GetTablesHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    // Create a DynamoDbClient
    private final DynamoDbClient dbClient = DynamoDbClient.builder()
            .region(Region.of(System.getenv("region")))
            .build();

    // Create an enhanced client
    private final DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(dbClient)
            .build();

    private final DynamoDbTable<Table> table = enhancedClient.table(System.getenv("tables_table"), TableSchema.fromBean(Table.class));

    @SneakyThrows
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent requestEvent, Context context) {
        context.getLogger().log("Tables request: list all tables");
        Tables tables= Tables.getTablesFromDb();
        context.getLogger().log("Return Tables info: " + tables);
        ObjectMapper mapper = new ObjectMapper();

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(200)
                .withBody(mapper.writeValueAsString(tables));
    }
}
