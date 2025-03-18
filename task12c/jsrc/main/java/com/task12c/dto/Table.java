package com.task12c.dto;

import lombok.Data;
import org.json.JSONException;
import org.json.JSONObject;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

@Data
@DynamoDbBean
public class Table {

    private int id;
    private int number;
    private int places;
    private boolean isVip = false;
    private int minOrder = 0;

    // @DynamoDbBean annotation requires existence of Default constructor
    public Table() {}

    @DynamoDbPartitionKey
    public int getId() {
        return id;
    }

    public Table (int id, int number, int places, boolean isVip, int minOrder) {
        if (id == 0 || number == 0 || places == 0) {
            throw new IllegalArgumentException("Missing or incomplete data.");
        }
        this.id = id;
        this.number = number;
        this.places = places;
        this.isVip = isVip;
        this.minOrder = minOrder;
    }

    public static Table fromJson(JSONObject jsonObject) {
        int id = jsonObject.getInt("id");
        int number = jsonObject.getInt("number");
        int places = jsonObject.getInt("places");
        boolean isVip;
        try {
            isVip = jsonObject.getBoolean("isVip");
        } catch (JSONException e) {
            isVip = false;
        }
        int minOrder;
        try {
            minOrder = jsonObject.getInt("minOrder");
        } catch (JSONException e) {
            minOrder = 0;
        }

        return new Table(id, number, places, isVip, minOrder);
    }

    public static Table fromJson(String jsonString) {
        JSONObject jsonObject = new JSONObject(jsonString);
        return fromJson(jsonObject);
    }
}
