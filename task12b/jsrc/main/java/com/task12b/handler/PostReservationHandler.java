package com.task12b.handler;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.task12b.dto.Reservation;
import com.task12b.dto.Reservations;
import com.task12b.dto.Tables;
import lombok.SneakyThrows;
import org.json.JSONObject;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;


public class PostReservationHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

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
    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent requestEvent, Context context) {
        context.getLogger().log("Reservation info: " + requestEvent.getBody());
        ObjectMapper mapper = new ObjectMapper();
        Reservation newReservation = mapper.readValue(requestEvent.getBody(), Reservation.class);
//        Reservation newReservation = Reservation.fromJson(requestEvent.getBody());
        context.getLogger().log("Parsed Reservation info: " + newReservation);
        if (!Tables.doesTableExist(newReservation.getTableNumber())) {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(400)
                    .withBody("{\"message\":\"Table with number " + newReservation.getTableNumber() + " does not exist.\"}");
        }
        if (Reservations.isOverlapping(newReservation)) {
            return new APIGatewayProxyResponseEvent()
                    .withStatusCode(400)
                    .withBody("{\"message\":\"Reservation overlaps with existing.\"}");
        }
        reservation.putItem(newReservation);
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(200)
                .withBody(new JSONObject()
                        .put("reservationId", newReservation.getReservationId())
                        .toString()
                );
    }
}
