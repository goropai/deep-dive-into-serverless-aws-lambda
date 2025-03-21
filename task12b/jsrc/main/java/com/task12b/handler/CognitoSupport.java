package com.task12b.handler;

import com.task12b.dto.SignUp;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.util.Map;

/**
 * Created by Roman Ivanov on 7/20/2024.
 */
public abstract class CognitoSupport {

    protected final String userPoolId = System.getenv("COGNITO_ID");
    protected final String clientId = System.getenv("CLIENT_ID");
    private final CognitoIdentityProviderClient cognitoClient;

    protected CognitoSupport(CognitoIdentityProviderClient cognitoClient) {
        this.cognitoClient = cognitoClient;
    }

    protected AdminInitiateAuthResponse cognitoSignIn(String nickName, String password) {
        Map<String, String> authParams = Map.of(
                "USERNAME", nickName,
                "PASSWORD", password
        );

        return cognitoClient.adminInitiateAuth(AdminInitiateAuthRequest.builder()
                .authFlow(AuthFlowType.ADMIN_NO_SRP_AUTH)
                .authParameters(authParams)
                .userPoolId(userPoolId)
                .clientId(clientId)
                .build());
    }

    protected AdminCreateUserResponse cognitoSignUp(SignUp signUp) {

        return cognitoClient.adminCreateUser(AdminCreateUserRequest.builder()
                .userPoolId(userPoolId)
                .username(signUp.email())
                .temporaryPassword(signUp.password())
                .userAttributes(
                        AttributeType.builder()
                                .name("given_name")
                                .value(signUp.firstName())
                                .build(),
                        AttributeType.builder()
                                .name("family_name")
                                .value(signUp.lastName())
                                .build(),
                        AttributeType.builder()
                                .name("email")
                                .value(signUp.email())
                                .build(),
                        AttributeType.builder()
                                .name("email_verified")
                                .value("true")
                                .build())
                .desiredDeliveryMediums(DeliveryMediumType.EMAIL)
                .messageAction("SUPPRESS")
                .forceAliasCreation(Boolean.FALSE)
                .build()
        );
    }

    protected AdminRespondToAuthChallengeResponse confirmSignUp(SignUp signUp) {
        AdminInitiateAuthResponse adminInitiateAuthResponse = cognitoSignIn(signUp.email(), signUp.password());

        if (!ChallengeNameType.NEW_PASSWORD_REQUIRED.name().equals(adminInitiateAuthResponse.challengeNameAsString())) {
            throw new RuntimeException("unexpected challenge: " + adminInitiateAuthResponse.challengeNameAsString());
        }

        Map<String, String> challengeResponses = Map.of(
                "USERNAME", signUp.email(),
                "PASSWORD", signUp.password(),
                "NEW_PASSWORD", signUp.password()
        );

        return cognitoClient.adminRespondToAuthChallenge(AdminRespondToAuthChallengeRequest.builder()
                .challengeName(ChallengeNameType.NEW_PASSWORD_REQUIRED)
                .challengeResponses(challengeResponses)
                .userPoolId(userPoolId)
                .clientId(clientId)
                .session(adminInitiateAuthResponse.session())
                .build());
    }

}
