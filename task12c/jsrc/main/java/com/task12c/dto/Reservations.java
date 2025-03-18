package com.task12c.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Data
public class Reservations {

    // Create a DynamoDbClient
    @JsonIgnore
    private final DynamoDbClient dbClient = DynamoDbClient.builder()
            .region(Region.of(System.getenv("region")))
            .build();

    // Create an enhanced client
    @JsonIgnore
    private final DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(dbClient)
            .build();

    @JsonIgnore
    private final DynamoDbTable<Reservation> reservation = enhancedClient.table(System.getenv("reservations_table"), TableSchema.fromBean(Reservation.class));

    @JsonProperty("reservations")
    private List<Reservation> reservations = new ArrayList<>();

    public void addReservation(Reservation reservation) {
        reservations.add(reservation);
    }

    public List<Reservation> getReservationsFromDb() {
        reservation.scan()
                .items()
                .forEach(this::addReservation);
        return reservations;
    }

    public static boolean isOverlapping(Reservation nr) {
        LocalTime nrStartTime = LocalTime.parse(nr.getSlotTimeStart());
        LocalTime nrEndTime = LocalTime.parse(nr.getSlotTimeEnd());

        Reservations reservations1 = new Reservations();
        List<Reservation> reservationList = reservations1.getReservationsFromDb();
        if (reservationList.isEmpty()) return false;
        return reservationList.stream()
                .filter((Reservation r) -> nr.getTableNumber() == r.getTableNumber())
                .anyMatch((Reservation r) -> {
                    LocalTime rStartTime = LocalTime.parse(r.getSlotTimeStart());
                    LocalTime rEndTime = LocalTime.parse(r.getSlotTimeEnd());
                    if (!Objects.equals(r.getDate(), nr.getDate())) return false;
                    return  !(nrStartTime.isAfter(rEndTime) && nrEndTime.isBefore(rStartTime));
                });
    }
}
