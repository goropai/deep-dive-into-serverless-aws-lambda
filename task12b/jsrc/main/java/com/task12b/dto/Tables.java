package com.task12b.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.ArrayList;
import java.util.List;

@Data
public class Tables {

    // Create a DynamoDbClient
    @JsonIgnore
    private static final DynamoDbClient dbClient = DynamoDbClient.builder()
            .region(Region.of(System.getenv("region")))
            .build();

    // Create an enhanced client
    @JsonIgnore
    private static final DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
            .dynamoDbClient(dbClient)
            .build();

    @JsonIgnore
    private static final DynamoDbTable<Table> table = enhancedClient.table(System.getenv("tables_table"), TableSchema.fromBean(Table.class));

    @JsonProperty("tables")
    private List<Table> tables = new ArrayList<>();

    public void addTable(Table table) {
        tables.add(table);
    }

    public static Tables getTablesFromDb() {
        List<Table> tablesFromDb = new ArrayList<>();
        table.scan()
                .items()
                .forEach(tablesFromDb::add);
        Tables currentTables = new Tables();
        currentTables.setTables(tablesFromDb);
        return currentTables;
    }

    public static boolean doesTableExist(int tableNumber) {
        return Tables.getTablesFromDb().getTables().stream().anyMatch((Table t) -> t.getNumber() == tableNumber);
    }
}
