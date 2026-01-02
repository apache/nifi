/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.oauth2;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.ECDSASigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTClaimsSet.Builder;
import com.nimbusds.jwt.SignedJWT;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.key.service.api.PrivateKeyService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.key.Ed25519Signer;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.web.client.api.HttpRequestHeadersSpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.RSAPrivateKey;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.net.ssl.X509ExtendedKeyManager;

@SupportsSensitiveDynamicProperties
@Tags({ "oauth2", "provider", "authorization", "access token", "hjwt" })
@CapabilityDescription("Provides OAuth 2.0 access tokens that can be used as Bearer authorization header in HTTP requests." +
        " This controller service is for implementing the OAuth 2.0 JWT Bearer Flow.")
@DynamicProperties({
        @DynamicProperty(
                name = "CLAIM.JWT claim name",
                value = "JWT claim value",
                expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
                description = "Custom claims that should be added to the JWT."),
        @DynamicProperty(
                name = "FORM.Request parameter name",
                value = "Request parameter value",
                expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
                description = "Custom parameters that should be added to the body of the request against the token endpoint.")
})
public class JWTBearerOAuth2AccessTokenProvider extends AbstractControllerService implements OAuth2AccessTokenProvider, VerifiableControllerService {

    public static final PropertyDescriptor TOKEN_ENDPOINT = new PropertyDescriptor.Builder()
            .name("Token Endpoint URL")
            .description("The URL of the OAuth2 token endpoint.")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor WEB_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Web Client Service")
            .description("The Web Client Service to use for calling the token endpoint.")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    public static final PropertyDescriptor PRIVATE_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("Private Key Service")
            .description("The private key service to use for signing JWTs.")
            .identifiesControllerService(PrivateKeyService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor SIGNING_ALGORITHM = new PropertyDescriptor.Builder()
            .name("Signing Algorithm")
            .description("The algorithm to use for signing the JWT.")
            .allowableValues(
                    JWSAlgorithm.RS256.getName(),
                    JWSAlgorithm.RS384.getName(),
                    JWSAlgorithm.RS512.getName(),
                    JWSAlgorithm.PS256.getName(),
                    JWSAlgorithm.PS384.getName(),
                    JWSAlgorithm.PS512.getName(),
                    JWSAlgorithm.ES256.getName(),
                    JWSAlgorithm.ES384.getName(),
                    JWSAlgorithm.ES512.getName(),
                    JWSAlgorithm.Ed25519.getName())
            .defaultValue(JWSAlgorithm.PS256.getName())
            .required(true)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor ISSUER = new PropertyDescriptor.Builder()
            .name("Issuer")
            .description("The issuer claim (iss) for the JWT.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SUBJECT = new PropertyDescriptor.Builder()
            .name("Subject")
            .description("The subject claim (sub) for the JWT.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor AUDIENCE = new PropertyDescriptor.Builder()
            .name("Audience")
            .description("The audience claim (aud) for the JWT. Space-separated list of audiences if multiple are expected.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor SCOPE = new PropertyDescriptor.Builder()
            .name("Scope")
            .description("The scope claim (scope) for the JWT.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor GRANT_TYPE = new PropertyDescriptor.Builder()
            .name("Grant Type")
            .description("Value to set for the grant_type parameter in the request to the token endpoint.")
            .required(true)
            .defaultValue("urn:ietf:params:oauth:grant-type:jwt-bearer")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor ASSERTION = new PropertyDescriptor.Builder()
            .name("Assertion Parameter Name")
            .description("Name of the parameter to use for the JWT assertion in the request to the token endpoint.")
            .required(true)
            .defaultValue("assertion")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor HEADER_X5T = new PropertyDescriptor.Builder()
            .name("Set JWT Header X.509 Cert Thumbprint")
            .description("""
                    If true, will set the JWT header x5t field with the base64url-encoded SHA-256 thumbprint of the X.509 certificate's DER encoding.
                    If set to true, an instance of SSLContextProvider must be configured with a certificate using RSA algorithm.
                    """)
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(Validator.VALID)
            .build();

    static final PropertyDescriptor SSL_CONTEXT_PROVIDER = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("An instance of SSLContextProvider configured with a certificate that will be used to set the x5t header. Must be using RSA algorithm.")
            .required(true)
            .dependsOn(HEADER_X5T, "true")
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    public static final PropertyDescriptor JTI = new PropertyDescriptor.Builder()
            .name("JWT ID")
            .description("""
                    The "jti" (JWT ID) claim provides a unique identifier for the JWT. The identifier value must be assigned in a
                    manner that ensures that there's a negligible probability that the same value will be accidentally assigned to a
                    different data object; if the application uses multiple issuers, collisions MUST be prevented among values produced
                    by different issuers as well. The \"jti\" value is a case-sensitive string. If set, it is recommended to set this
                    value to ${UUID()}.
                    """)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor KID = new PropertyDescriptor.Builder()
            .name("Key ID")
            .description("The ID of the public key used to sign the JWT. It'll be used as the kid header in the JWT.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    static final PropertyDescriptor REFRESH_WINDOW = new PropertyDescriptor.Builder()
            .name("Refresh Window")
            .description("The service will attempt to refresh tokens expiring within the refresh window, subtracting the configured duration from the token expiration.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 minutes")
            .required(true)
            .build();

    static final PropertyDescriptor JWT_VALIDITY = new PropertyDescriptor.Builder()
            .name("JWT Expiration Time")
            .description("""
                    Expiration time used to set the corresponding claim of the JWT. In case the returned access token does not include
                    an expiration time, this will be used with the refresh window to re-acquire a new access token.
                    """)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("1 hour")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            TOKEN_ENDPOINT,
            WEB_CLIENT_SERVICE,
            PRIVATE_KEY_SERVICE,
            SIGNING_ALGORITHM,
            ISSUER,
            SUBJECT,
            AUDIENCE,
            SCOPE,
            JTI,
            HEADER_X5T,
            SSL_CONTEXT_PROVIDER,
            KID,
            GRANT_TYPE,
            ASSERTION,
            REFRESH_WINDOW,
            JWT_VALIDITY
    );

    private static final ObjectMapper ACCESS_DETAILS_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .addMixIn(AccessToken.class, AccessTokenAdditionalParameters.class)
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    private volatile AccessToken accessDetails;
    private volatile JWSSigner signer;
    private volatile WebClientService webClientService;
    private volatile PrivateKey privateKey;
    private volatile X509ExtendedKeyManager keyManager;
    private volatile String tokenEndpoint;
    private volatile Duration refreshWindow;
    private volatile Duration jwtValidity;
    private volatile String issuer;
    private volatile String subject;
    private volatile String audience;
    private volatile String algorithmName;
    private volatile String scope;
    private volatile Supplier<String> jti;
    private volatile String kid;
    private volatile String grantType;
    private volatile String assertion;
    private volatile boolean headerX5T;

    private volatile Map<String, String> customClaims;
    private volatile Map<String, String> formParams;

    static final String CLAIM_PREFIX = "CLAIM.";
    static final String FORM_PREFIX = "FORM.";

    private static final String APPLICATION_JSON = "application/json";
    private static final String APPLICATION_URLENCODED = "application/x-www-form-urlencoded";

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        initProperties(context);
        initJWTSigner();
    }

    @OnDisabled
    public void onDisabled() {
        accessDetails = null;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if (propertyDescriptorName.startsWith(CLAIM_PREFIX)) {
            return new PropertyDescriptor.Builder()
                    .required(false)
                    .name(propertyDescriptorName)
                    .description("The value of the claim to add to the JWT.")
                    .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                    .dynamic(true)
                    .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                    .build();
        }

        if (propertyDescriptorName.startsWith(FORM_PREFIX)) {
            return new PropertyDescriptor.Builder()
                    .required(false)
                    .name(propertyDescriptorName)
                    .description("The value of the form parameter to add to the request body.")
                    .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                    .dynamic(true)
                    .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                    .build();
        }

        return null;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        PrivateKeyService keyService = validationContext.getProperty(PRIVATE_KEY_SERVICE).asControllerService(PrivateKeyService.class);
        String algorithmName = validationContext.getProperty(SIGNING_ALGORITHM).getValue();
        PrivateKey privateKey = keyService.getPrivateKey();
        JWSAlgorithm algorithm = JWSAlgorithm.parse(algorithmName);

        if (validationContext.getProperty(HEADER_X5T).asBoolean() && !(privateKey instanceof RSAPrivateKey)) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(HEADER_X5T.getDisplayName())
                    .valid(false)
                    .explanation("The private key must be an RSA key to set the x5t header.")
                    .build());
        }

        if (privateKey instanceof RSAPrivateKey) {
            if (!algorithmName.startsWith("RS")
                    && !algorithmName.startsWith("PS")) {
                validationResults.add(new ValidationResult.Builder()
                        .subject(SIGNING_ALGORITHM.getDisplayName())
                        .valid(false)
                        .explanation("The private key is of RSA type, the signing algorithm must be either RS256 or PS256.")
                        .build());
            }
        } else if (privateKey instanceof ECPrivateKey) {
            if (!algorithm.equals(JWSAlgorithm.ES256)
                    && !algorithm.equals(JWSAlgorithm.ES384)
                    && !algorithm.equals(JWSAlgorithm.ES512)) {
                validationResults.add(new ValidationResult.Builder()
                        .subject(SIGNING_ALGORITHM.getDisplayName())
                        .valid(false)
                        .explanation("The private key is of EC type, the signing algorithm must be either ES256, ES384 or ES512.")
                        .build());
            }
        } else if (!algorithm.equals(JWSAlgorithm.Ed25519)) {
            validationResults.add(new ValidationResult.Builder()
                    .subject(SIGNING_ALGORITHM.getDisplayName())
                    .valid(false)
                    .explanation(String.format("The provided key (algorithm = %s) is not supported for signing algorithm %s", privateKey.getAlgorithm(), algorithmName))
                    .build());
        }

        return validationResults;
    }

    @Override
    public List<ConfigVerificationResult> verify(ConfigurationContext context, ComponentLog verificationLogger, Map<String, String> variables) {
        initProperties(context);
        initJWTSigner();
        ConfigVerificationResult.Builder builder = new ConfigVerificationResult.Builder().verificationStepName("Acquire token");
        try {
            getAccessDetails();
            builder.outcome(ConfigVerificationResult.Outcome.SUCCESSFUL);
        } catch (final Exception ex) {
            String explanation = ex.getMessage();
            if (ex.getCause() != null) {
                explanation += " (" + ex.getCause().getMessage() + ")";
            }
            builder.outcome(ConfigVerificationResult.Outcome.FAILED).explanation(explanation);
        }
        return Arrays.asList(builder.build());
    }

    @Override
    public AccessToken getAccessDetails() {
        if (this.accessDetails == null || isRefreshRequired()) {
            try {
                acquireAccessDetails();
            } catch (final Exception e) {
                throw new AccessTokenRetrievalException("Failed to acquire Access Token", e);
            }
        }
        return accessDetails;
    }

    private boolean isRefreshRequired() {
        if (accessDetails.getExpiresIn() != null) {
            final Instant expirationRefreshTime = accessDetails.getFetchTime()
                    .plusSeconds(accessDetails.getExpiresIn())
                    .minus(refreshWindow);

            return Instant.now().isAfter(expirationRefreshTime);
        } else {
            final Instant expirationRefreshTime = accessDetails.getFetchTime()
                    .plusSeconds(jwtValidity.getSeconds())
                    .minus(refreshWindow);

            return Instant.now().isAfter(expirationRefreshTime);
        }
    }

    private void acquireAccessDetails() throws URISyntaxException, JOSEException {
        getLogger().debug("New Access Token request started [{}]", tokenEndpoint);

        final Instant now = Instant.now();
        final Date nowDate = Date.from(now);
        final Date expirationTime = Date.from(now.plus(jwtValidity));

        Builder claimsSetBuilder = new JWTClaimsSet.Builder()
                .expirationTime(expirationTime)
                .issueTime(nowDate)
                .notBeforeTime(nowDate);

        if (issuer != null) {
            claimsSetBuilder.issuer(issuer);
        }

        if (subject != null) {
            claimsSetBuilder.subject(subject);
        }

        if (audience != null) {
            claimsSetBuilder.audience(Arrays.asList(audience.split(" ")));
        }

        if (scope != null) {
            claimsSetBuilder.claim("scope", scope);
        }

        if (jti != null) {
            claimsSetBuilder.jwtID(jti.get());
        }

        customClaims.forEach(claimsSetBuilder::claim);

        JWSHeader.Builder headerBuilder = new JWSHeader.Builder(JWSAlgorithm.parse(algorithmName));
        headerBuilder.type(JOSEObjectType.JWT);

        if (kid != null) {
            headerBuilder.keyID(kid);
        }

        if (headerX5T) {
            try {
                final String url = getBase64EncodedSHA256Digest();
                headerBuilder.x509CertSHA256Thumbprint(new Base64URL(url));
            } catch (final AccessTokenRetrievalException e) {
                throw e;
            } catch (final Exception e) {
                throw new AccessTokenRetrievalException("Failed to set x5t header", e);
            }
        }

        Map<String, String> formParams = new HashMap<>();
        formParams.put("grant_type", grantType);
        formParams.put(assertion, getAssertion(headerBuilder.build(), claimsSetBuilder.build()));
        formParams.putAll(this.formParams);

        requestTokenEndpoint(formParams);
    }

    private String getBase64EncodedSHA256Digest() throws NoSuchAlgorithmException, CertificateEncodingException {
        final String alias = keyManager.chooseClientAlias(new String[] {"RSA"}, null, null);
        if (alias == null) {
            throw new AccessTokenRetrievalException("Cannot set x5t header because no key alias found");
        } else {
            final PrivateKey privateKey = keyManager.getPrivateKey(alias);
            if (privateKey == null) {
                throw new AccessTokenRetrievalException(String.format("Cannot set x5t header because no private key found for alias %s", alias));
            } else {
                final X509Certificate[] certificates = keyManager.getCertificateChain(alias);
                if (certificates == null || certificates.length == 0) {
                    throw new AccessTokenRetrievalException(String.format("Cannot set x5t header because no certificate chain found for alias %s", alias));
                } else {
                    final MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
                    final byte[] bytes = messageDigest.digest(certificates[0].getEncoded());
                    return Base64.getEncoder().encodeToString(bytes);
                }
            }
        }
    }

    protected void requestTokenEndpoint(Map<String, String> formParams) throws URISyntaxException {
        HttpRequestHeadersSpec request = webClientService
                .post()
                .uri(new URI(tokenEndpoint))
                .header("Accept", APPLICATION_JSON)
                .header("Content-Type", APPLICATION_URLENCODED)
                .body(formParams.entrySet()
                        .stream()
                        .map(param -> param.getKey() + "=" + param.getValue())
                        .collect(Collectors.joining("&")));

        try (final HttpResponseEntity response = request.retrieve()) {
            if (response.statusCode() != 200) {
                String body;
                try (final InputStream is = response.body()) {
                    body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    body = "[failed to read response: " + e.getMessage() + "]";
                }
                final String message = "Failed to retrieve Access Token from [%s]: HTTP %s with Response [%s]".formatted(
                        tokenEndpoint,
                        response.statusCode(),
                        body);
                throw new AccessTokenRetrievalException(message);
            }

            try (final InputStream body = response.body()) {
                accessDetails = ACCESS_DETAILS_MAPPER.readValue(body, AccessToken.class);
            }
        } catch (final IOException e) {
            throw new AccessTokenRetrievalException("Failed to retrieve or process access token details", e);
        }
    }

    protected String getAssertion(JWSHeader jwsHeader, JWTClaimsSet jwtClaimsSet) throws JOSEException {
        SignedJWT signedJWT = new SignedJWT(jwsHeader, jwtClaimsSet);
        signedJWT.sign(signer);
        return signedJWT.serialize();
    }

    private void initProperties(ConfigurationContext context) {
        privateKey = context.getProperty(PRIVATE_KEY_SERVICE).asControllerService(PrivateKeyService.class).getPrivateKey();
        tokenEndpoint = context.getProperty(TOKEN_ENDPOINT).getValue();
        webClientService = context.getProperty(WEB_CLIENT_SERVICE).asControllerService(WebClientServiceProvider.class).getWebClientService();
        refreshWindow = context.getProperty(REFRESH_WINDOW).asDuration();
        jwtValidity = context.getProperty(JWT_VALIDITY).asDuration();
        tokenEndpoint = context.getProperty(TOKEN_ENDPOINT).getValue();
        algorithmName = context.getProperty(SIGNING_ALGORITHM).getValue();
        headerX5T = context.getProperty(HEADER_X5T).asBoolean();
        grantType = context.getProperty(GRANT_TYPE).evaluateAttributeExpressions().getValue();
        assertion = context.getProperty(ASSERTION).evaluateAttributeExpressions().getValue();

        if (context.getProperty(ISSUER).isSet()) {
            issuer = context.getProperty(ISSUER).evaluateAttributeExpressions().getValue();
        } else {
            issuer = null;
        }

        if (context.getProperty(SUBJECT).isSet()) {
            subject = context.getProperty(SUBJECT).evaluateAttributeExpressions().getValue();
        } else {
            subject = null;
        }

        if (context.getProperty(AUDIENCE).isSet()) {
            audience = context.getProperty(AUDIENCE).evaluateAttributeExpressions().getValue();
        } else {
            audience = null;
        }

        if (context.getProperty(SCOPE).isSet()) {
            scope = context.getProperty(SCOPE).evaluateAttributeExpressions().getValue();
        } else {
            scope = null;
        }

        if (context.getProperty(JTI).isSet()) {
            jti = () -> context.getProperty(JTI).evaluateAttributeExpressions().getValue();
        } else {
            jti = null;
        }

        if (context.getProperty(KID).isSet()) {
            kid = context.getProperty(KID).evaluateAttributeExpressions().getValue();
        } else {
            kid = null;
        }

        if (context.getProperty(SSL_CONTEXT_PROVIDER).isSet()) {
            SSLContextProvider sslContextProvider = context.getProperty(SSL_CONTEXT_PROVIDER).asControllerService(SSLContextProvider.class);
            keyManager = sslContextProvider.createKeyManager().orElseThrow(() -> new IllegalStateException("KeyManager not available"));
        }

        customClaims = new HashMap<>();
        formParams = new HashMap<>();
        for (PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                if (descriptor.getName().startsWith(CLAIM_PREFIX)) {
                    customClaims.put(StringUtils.substringAfter(descriptor.getName(), CLAIM_PREFIX),
                            context.getProperty(descriptor).evaluateAttributeExpressions().getValue());
                } else if (descriptor.getName().startsWith(FORM_PREFIX)) {
                    formParams.put(StringUtils.substringAfter(descriptor.getName(), FORM_PREFIX),
                            URLEncoder.encode(context.getProperty(descriptor).evaluateAttributeExpressions().getValue(), StandardCharsets.UTF_8));
                }
            }
        }
    }

    private void initJWTSigner() {
        final JWSAlgorithm algorithm = JWSAlgorithm.parse(algorithmName);

        if (privateKey instanceof RSAPrivateKey rsaPrivateKey) {
            signer = new RSASSASigner(rsaPrivateKey);
        } else if (privateKey instanceof ECPrivateKey ecPrivateKey) {
            try {
                signer = new ECDSASigner(ecPrivateKey);
            } catch (final JOSEException e) {
                throw new IllegalArgumentException("Failed to create ECDSA signer", e);
            }
        } else if (algorithm.equals(JWSAlgorithm.Ed25519)) {
            signer = new Ed25519Signer(privateKey);
        } else {
            throw new IllegalArgumentException(String.format("The provided key (algorithm = %s) is not supported for signing algorithm %s", privateKey.getAlgorithm(), algorithmName));
        }
    }

    private final class AccessTokenRetrievalException extends RuntimeException {
        public AccessTokenRetrievalException(final String message) {
            super(message);
        }

        public AccessTokenRetrievalException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    interface AccessTokenAdditionalParameters {

        @JsonAnySetter
        void setAdditionalParameter(String key, Object value);
    }
}
