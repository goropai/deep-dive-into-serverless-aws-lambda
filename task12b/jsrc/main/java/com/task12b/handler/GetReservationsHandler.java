package com.task12b.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.task12b.dto.Reservation;
import com.task12b.dto.Reservations;
import lombok.SneakyThrows;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class GetReservationsHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    // Create a DynamoDbClient
    private final DynamoDbClient dbClient = DynamoDbClient.builder()
            .region(Region.of(System.getenv("region")))
            .build();

    // Create an enhanced client
    private final DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(dbClient)
            .build();

    private final DynamoDbTable<Reservation> reservation = enhancedClient.table(System.getenv("reservations_table"), TableSchema.fromBean(Reservation.class));

    @SneakyThrows
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent requestEvent, Context context) {
        context.getLogger().log("Reservations request: list all reservations");
        Reservations reservations = new Reservations();
        reservation.scan()
                .items()
                .forEach(reservations::addReservation);
        context.getLogger().log("Return Reservations info: " + reservations);
        ObjectMapper mapper = new ObjectMapper();

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(200)
                .withBody(mapper.writeValueAsString(reservations));
    }

}
