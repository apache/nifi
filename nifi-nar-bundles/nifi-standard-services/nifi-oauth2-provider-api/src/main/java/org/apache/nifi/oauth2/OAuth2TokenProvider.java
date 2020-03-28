package org.apache.nifi.oauth2;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.ssl.SSLContextService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Interface for defining a credential-providing controller service for oauth2 processes.
 */
public interface OAuth2TokenProvider extends ControllerService {
    PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("oauth2-ssl-context")
            .displayName("SSL Context")
            .addValidator(Validator.VALID)
            .identifiesControllerService(SSLContextService.class)
            .required(false)
            .build();
    PropertyDescriptor ACCESS_TOKEN_URL = new PropertyDescriptor.Builder()
            .name("oauth2-access-token-url")
            .displayName("Access Token Url")
            .description("The full endpoint of the URL where access tokens are handled.")
            .required(false)
            .defaultValue("")
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            SSL_CONTEXT, ACCESS_TOKEN_URL
    ));

    AccessToken getAccessTokenByPassword(String clientId, String clientSecret, String username, String password);
    AccessToken getAccessTokenByClientCredentials(String clientId, String clientSecret);
    AccessToken refreshToken(AccessToken refreshThis);
}
