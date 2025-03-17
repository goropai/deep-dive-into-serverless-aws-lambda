package com.task11.utils;

import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.util.Optional;

public class CognitoHelper {

    private final CognitoIdentityProviderClient cognitoClient;

    public CognitoHelper() {
        this.cognitoClient = CognitoIdentityProviderClient.builder()
                .region(Region.of(System.getenv("region")))
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();
    }

    public Optional<String> getUserPoolIdByName(String userPoolName) {
        ListUserPoolsRequest listUserPoolsRequest = ListUserPoolsRequest.builder().build();
        ListUserPoolsResponse listUserPoolsResponse = cognitoClient.listUserPools(listUserPoolsRequest);

        for (UserPoolDescriptionType pool : listUserPoolsResponse.userPools()) {
            if (pool.name().equals(userPoolName)) {
                return Optional.of(pool.id());
            }
        }

        return Optional.empty();
    }

    public Optional<String> getClientIdByUserPoolName(String userPoolName) {
        String userPoolId = getUserPoolIdByName(userPoolName).get();

        ListUserPoolClientsRequest listUserPoolClientsRequest =
                ListUserPoolClientsRequest.builder().userPoolId(userPoolId).build();
        ListUserPoolClientsResponse listUserPoolClientsResponse = cognitoClient.listUserPoolClients(listUserPoolClientsRequest);

        for (UserPoolClientDescription client : listUserPoolClientsResponse.userPoolClients()) {
            return Optional.of(client.clientId());
        }

        return Optional.empty();
    }
}
