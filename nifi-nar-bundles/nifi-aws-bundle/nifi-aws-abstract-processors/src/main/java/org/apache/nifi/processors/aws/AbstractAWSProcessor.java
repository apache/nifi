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
package org.apache.nifi.processors.aws;

import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY;
import static com.amazonaws.retry.PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION;

import com.amazonaws.AmazonWebServiceClient;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.http.conn.ssl.SdkTLSSocketFactory;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.PredefinedBackoffStrategies.EqualJitterBackoffStrategy;
import com.amazonaws.retry.PredefinedBackoffStrategies.ExponentialBackoffStrategy;
import com.amazonaws.retry.PredefinedBackoffStrategies.FullJitterBackoffStrategy;
import com.amazonaws.retry.PredefinedBackoffStrategies.SDKDefaultBackoffStrategy;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.retry.RetryPolicy.BackoffStrategy;
import com.amazonaws.retry.RetryPolicy.RetryCondition;
import com.amazonaws.services.s3.internal.CompleteMultipartUploadRetryCondition;

import java.io.File;
import java.io.IOException;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.factory.CredentialPropertyDescriptors;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.ssl.SSLContextService;

/**
 * Abstract base class for aws processors.  This class uses aws credentials for creating aws clients
 *
 * @deprecated use {@link AbstractAWSCredentialsProviderProcessor} instead which uses credentials providers or creating aws clients
 * @see AbstractAWSCredentialsProviderProcessor
 *
 */
@Deprecated
public abstract class AbstractAWSProcessor<ClientType extends AmazonWebServiceClient> extends AbstractProcessor {

    public static final String BACKOFF_STRATEGY_EQUAL_JITTER = "EqualJitterBackoffStrategy";
    public static final String BACKOFF_STRATEGY_EXPONENTIAL = "ExponentialBackoffStrategy";
    public static final String BACKOFF_STRATEGY_FULL_JITTER = "FullJitterBackoffStrategy";
    public static final String BACKOFF_STRATEGY_NO_DELAY = "NoDelayBackoffStrategy";
    public static final String BACKOFF_STRATEGY_SDKDEFAULT = "SDKDefaultBackoffStrategy";
    public static final String RETRY_CONDITION_COMPLETE_MULTIPART_UPLOAD = "CompleteMultipartUploadRetryCondition";
    public static final String RETRY_CONDITION_NO_RETRY = "NoRetryCondition";
    public static final String RETRY_CONDITION_SDKDEFAULT = "SDKDefaultRetryCondition";
    public static final String RETRY_POLICY_CUSTOM = "custom";
    public static final String RETRY_POLICY_DYNAMODB_DEFAULT = "dynamodb_default";
    public static final String RETRY_POLICY_DEFAULT = "default";

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles are routed to success relationship").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("FlowFiles are routed to failure relationship").build();

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    public static final PropertyDescriptor CREDENTIALS_FILE = CredentialPropertyDescriptors.CREDENTIALS_FILE;
    public static final PropertyDescriptor ACCESS_KEY = CredentialPropertyDescriptors.ACCESS_KEY;
    public static final PropertyDescriptor SECRET_KEY = CredentialPropertyDescriptors.SECRET_KEY;

    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
            .name("Proxy Host")
            .description("Proxy host name or IP")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_HOST_PORT = new PropertyDescriptor.Builder()
            .name("Proxy Host Port")
            .description("Proxy host port")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_USERNAME = new PropertyDescriptor.Builder()
            .name("proxy-user-name")
            .displayName("Proxy Username")
            .description("Proxy username")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_PASSWORD = new PropertyDescriptor.Builder()
            .name("proxy-user-password")
            .displayName("Proxy Password")
            .description("Proxy password")
            .expressionLanguageSupported(true)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor REGION = new PropertyDescriptor.Builder()
            .name("Region")
            .required(true)
            .allowableValues(getAvailableRegions())
            .defaultValue(createAllowableValue(Regions.DEFAULT_REGION).getValue())
            .build();

    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 secs")
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("Specifies an optional SSL Context Service that, if provided, will be used to create connections")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor ENDPOINT_OVERRIDE = new PropertyDescriptor.Builder()
            .name("Endpoint Override URL")
            .description("Endpoint URL to use instead of the AWS default including scheme, host, port, and path. " +
                    "The AWS libraries select an endpoint URL based on the AWS region, but this property overrides " +
                    "the selected endpoint URL, allowing use with other S3-compatible endpoints.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor AWS_MAX_ERROR_RETRY = new PropertyDescriptor.Builder()
      .name("max-error-retry")
      .displayName("Max Error Retry")
      .description("Maximum number of retry attempts for failed requests.")
      .defaultValue("0")
      .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
      .build();

    public static final PropertyDescriptor AWS_RETRY_POLICY = new PropertyDescriptor.Builder()
      .name("retry-policy")
      .displayName("Retry Policy")
      .description("Retry policy that can be configured on a specific service client using ClientConfiguration.")
      .allowableValues(new HashSet<>(Arrays.asList(
        RETRY_POLICY_DEFAULT, RETRY_POLICY_DYNAMODB_DEFAULT, RETRY_POLICY_DEFAULT)))
      .defaultValue(RETRY_POLICY_DEFAULT)
      .build();

    public static final PropertyDescriptor AWS_RETRY_CONDITION = new PropertyDescriptor.Builder()
      .name("retry-condition")
      .displayName("Retry Condition")
      .description("Retry condition on whether a specific request and exception should be retried.")
      .allowableValues(new HashSet<>(Arrays.asList(
        RETRY_CONDITION_SDKDEFAULT, RETRY_CONDITION_COMPLETE_MULTIPART_UPLOAD, RETRY_CONDITION_NO_RETRY)))
      .defaultValue(RETRY_CONDITION_SDKDEFAULT)
      .build();

    public static final PropertyDescriptor AWS_BACKOFF_STRATEGY = new PropertyDescriptor.Builder()
      .name("backoff-strategy")
      .displayName("Backoff Strategy")
      .description("Back-off strategy for controlling how long the next retry should wait.")
      .allowableValues(new HashSet<>(Arrays.asList(
        BACKOFF_STRATEGY_EQUAL_JITTER,BACKOFF_STRATEGY_EXPONENTIAL, BACKOFF_STRATEGY_FULL_JITTER,
        BACKOFF_STRATEGY_NO_DELAY, BACKOFF_STRATEGY_SDKDEFAULT )))
      .defaultValue(BACKOFF_STRATEGY_SDKDEFAULT)
      .build();

    public static final PropertyDescriptor AWS_BACKOFF_BASE_DELAY = new PropertyDescriptor.Builder()
      .name("backoff-base-delay")
      .displayName("Back-off Base Delay")
      .description("Base sleep time (milliseconds) for non-throttled exceptions")
      .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
      .defaultValue("100")
      .build();

    public static final PropertyDescriptor AWS_BACKOFF_THROTTLED_BASE_DELAY = new PropertyDescriptor.Builder()
      .name("backoff-throttled-base-delay")
      .displayName("Back-off Throttled Base Delay")
      .description("Base sleep time (milliseconds) for throttled exceptions")
      .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
      .defaultValue("500")
      .build();

    public static final PropertyDescriptor AWS_BACKOFF_MAX_BACKOFF_TIME = new PropertyDescriptor.Builder()
      .name("backoff-max-backoff-time")
      .displayName("Max Back-off Time")
      .description("Maximum back-off time before retrying a request")
      .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
      .defaultValue("20000")
      .build();

    protected volatile ClientType client;
    protected volatile Region region;

    // If protocol is changed to be a property, ensure other uses are also changed
    protected static final Protocol DEFAULT_PROTOCOL = Protocol.HTTPS;
    protected static final String DEFAULT_USER_AGENT = "NiFi";

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);

    public static AllowableValue createAllowableValue(final Regions region) {
        return new AllowableValue(region.getName(), region.getDescription(), "AWS Region Code : " + region.getName());
    }

    public static AllowableValue[] getAvailableRegions() {
        final List<AllowableValue> values = new ArrayList<>();
        for (final Regions region : Regions.values()) {
            values.add(createAllowableValue(region));
        }
        return values.toArray(new AllowableValue[values.size()]);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        final boolean accessKeySet = validationContext.getProperty(ACCESS_KEY).isSet();
        final boolean secretKeySet = validationContext.getProperty(SECRET_KEY).isSet();
        if ((accessKeySet && !secretKeySet) || (secretKeySet && !accessKeySet)) {
            problems.add(new ValidationResult.Builder().input("Access Key").valid(false).explanation("If setting Secret Key or Access Key, must set both").build());
        }

        final boolean credentialsFileSet = validationContext.getProperty(CREDENTIALS_FILE).isSet();
        if ((secretKeySet || accessKeySet) && credentialsFileSet) {
            problems.add(new ValidationResult.Builder().input("Access Key").valid(false).explanation("Cannot set both Credentials File and Secret Key/Access Key").build());
        }

        final boolean proxyHostSet = validationContext.getProperty(PROXY_HOST).isSet();
        final boolean proxyPortSet = validationContext.getProperty(PROXY_HOST_PORT).isSet();

        if ((proxyHostSet && !proxyPortSet) || (!proxyHostSet && proxyPortSet)) {
            problems.add(new ValidationResult.Builder().subject("Proxy Host and Port").valid(false).explanation("If Proxy Host or Proxy Port is set, both must be set").build());
        }

        final boolean proxyUserSet = validationContext.getProperty(PROXY_USERNAME).isSet();
        final boolean proxyPwdSet = validationContext.getProperty(PROXY_PASSWORD).isSet();

        if ((proxyUserSet && !proxyPwdSet) || (!proxyUserSet && proxyPwdSet)) {
            problems.add(new ValidationResult.Builder().subject("Proxy User and Password").valid(false).explanation("If Proxy Username or Proxy Password is set, both must be set").build());
        }
        if (proxyUserSet && !proxyHostSet) {
            problems.add(new ValidationResult.Builder().subject("Proxy").valid(false).explanation("If Proxy username is set, proxy host must be set").build());
        }

        ProxyConfiguration.validateProxySpec(validationContext, problems, PROXY_SPECS);

        return problems;
    }

    protected ClientConfiguration createConfiguration(final ProcessContext context) {
        final ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(context.getMaxConcurrentTasks());
        PropertyValue property = context.getProperty(AWS_MAX_ERROR_RETRY);
        config.setMaxErrorRetry(property.isSet()? property.asInteger() : 0);
        setRetryPolicy(config, context);
        config.setUserAgent(DEFAULT_USER_AGENT);
        // If this is changed to be a property, ensure other uses are also changed
        config.setProtocol(DEFAULT_PROTOCOL);
        final int commsTimeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        config.setConnectionTimeout(commsTimeout);
        config.setSocketTimeout(commsTimeout);

        if(this.getSupportedPropertyDescriptors().contains(SSL_CONTEXT_SERVICE)) {
            final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            if (sslContextService != null) {
                final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE);
                // NIFI-3788: Changed hostnameVerifier from null to DHV (BrowserCompatibleHostnameVerifier is deprecated)
                SdkTLSSocketFactory sdkTLSSocketFactory = new SdkTLSSocketFactory(sslContext, new DefaultHostnameVerifier());
                config.getApacheHttpClientConfig().setSslSocketFactory(sdkTLSSocketFactory);
            }
        }

        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context, () -> {
            if (context.getProperty(PROXY_HOST).isSet()) {
                final ProxyConfiguration componentProxyConfig = new ProxyConfiguration();
                String proxyHost = context.getProperty(PROXY_HOST).evaluateAttributeExpressions().getValue();
                Integer proxyPort = context.getProperty(PROXY_HOST_PORT).evaluateAttributeExpressions().asInteger();
                String proxyUsername = context.getProperty(PROXY_USERNAME).evaluateAttributeExpressions().getValue();
                String proxyPassword = context.getProperty(PROXY_PASSWORD).evaluateAttributeExpressions().getValue();
                componentProxyConfig.setProxyType(Proxy.Type.HTTP);
                componentProxyConfig.setProxyServerHost(proxyHost);
                componentProxyConfig.setProxyServerPort(proxyPort);
                componentProxyConfig.setProxyUserName(proxyUsername);
                componentProxyConfig.setProxyUserPassword(proxyPassword);
                return componentProxyConfig;
            }
            return ProxyConfiguration.DIRECT_CONFIGURATION;
        });

        if (Proxy.Type.HTTP.equals(proxyConfig.getProxyType())) {
            config.setProxyHost(proxyConfig.getProxyServerHost());
            config.setProxyPort(proxyConfig.getProxyServerPort());

            if (proxyConfig.hasCredential()) {
                config.setProxyUsername(proxyConfig.getProxyUserName());
                config.setProxyPassword(proxyConfig.getProxyUserPassword());
            }
        }

        return config;
    }
    private void setRetryPolicy(ClientConfiguration config, ProcessContext context) {
        PropertyValue property = context.getProperty(AWS_RETRY_POLICY);
        if (property.isSet()) {
            switch (property.getValue()){
                case RETRY_POLICY_CUSTOM:
                    config.setRetryPolicy(new RetryPolicy(
                      getRetryCondition(context),
                      getBackoffStrategy(context),
                      getMaxErrorRetry(context),
                      true
                    ));
                    break;
                case RETRY_POLICY_DYNAMODB_DEFAULT:
                    config.setRetryPolicy(PredefinedRetryPolicies.DYNAMODB_DEFAULT);
                    break;
                case RETRY_POLICY_DEFAULT:
                default:
                    break;
            }
        }
    }

    private RetryCondition getRetryCondition(ProcessContext context) {
        PropertyValue property = context.getProperty(AWS_RETRY_CONDITION);

        if (!property.isSet()) {
            return DEFAULT_RETRY_CONDITION;
        }

        switch (property.getValue()) {
            case RETRY_CONDITION_COMPLETE_MULTIPART_UPLOAD:
                return new CompleteMultipartUploadRetryCondition();
            case RETRY_CONDITION_NO_RETRY:
                return RetryCondition.NO_RETRY_CONDITION;
            case RETRY_CONDITION_SDKDEFAULT:
            default:
                return DEFAULT_RETRY_CONDITION;
        }
    }

    private BackoffStrategy getBackoffStrategy(ProcessContext context) {
        PropertyValue property = context.getProperty(AWS_BACKOFF_STRATEGY);
        if (property.isSet()) {
            int baseDelay = getPropertyValueOrDefault(context, AWS_BACKOFF_BASE_DELAY, 100);
            int maxBackoffTime = getPropertyValueOrDefault(context, AWS_BACKOFF_MAX_BACKOFF_TIME, 20000);
            int throttledBaseDelay = getPropertyValueOrDefault(context, AWS_BACKOFF_THROTTLED_BASE_DELAY, 500);
            switch (property.getValue()) {
                case BACKOFF_STRATEGY_EQUAL_JITTER:
                    return new EqualJitterBackoffStrategy(baseDelay, maxBackoffTime);
                case BACKOFF_STRATEGY_EXPONENTIAL:
                    return new ExponentialBackoffStrategy(baseDelay, maxBackoffTime);
                case BACKOFF_STRATEGY_FULL_JITTER:
                    return new FullJitterBackoffStrategy(baseDelay, maxBackoffTime);
                case BACKOFF_STRATEGY_NO_DELAY:
                    return BackoffStrategy.NO_DELAY;
                case BACKOFF_STRATEGY_SDKDEFAULT:
                default:
                    return new SDKDefaultBackoffStrategy(baseDelay, throttledBaseDelay, maxBackoffTime);
            }
        }

        return DEFAULT_BACKOFF_STRATEGY;
    }

    private int getMaxErrorRetry(ProcessContext context) {
        return getPropertyValueOrDefault(context, AWS_MAX_ERROR_RETRY, 0);
    }

    private Integer getPropertyValueOrDefault(ProcessContext context, PropertyDescriptor descriptor, int defaultValue) {
        return Optional.of(context.getProperty(descriptor))
          .filter(PropertyValue::isSet)
          .map(PropertyValue::asInteger)
          .orElse(defaultValue);
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final ClientType awsClient = createClient(context, getCredentials(context), createConfiguration(context));
        this.client = awsClient;
        initializeRegionAndEndpoint(context);
    }

    protected void initializeRegionAndEndpoint(ProcessContext context) {
        // if the processor supports REGION, get the configured region.
        if (getSupportedPropertyDescriptors().contains(REGION)) {
            final String region = context.getProperty(REGION).getValue();
            if (region != null) {
                this.region = Region.getRegion(Regions.fromName(region));
                client.setRegion(this.region);
            } else {
                this.region = null;
            }
        }

        // if the endpoint override has been configured, set the endpoint.
        // (per Amazon docs this should only be configured at client creation)
        if (getSupportedPropertyDescriptors().contains(ENDPOINT_OVERRIDE)) {
            final String urlstr = StringUtils.trimToEmpty(context.getProperty(ENDPOINT_OVERRIDE).evaluateAttributeExpressions().getValue());

            if (!urlstr.isEmpty()) {
                getLogger().info("Overriding endpoint with {}", new Object[]{urlstr});

                if (urlstr.endsWith(".vpce.amazonaws.com")) {
                    String region = parseRegionForVPCE(urlstr);
                    this.client.setEndpoint(urlstr, this.client.getServiceName(), region);
                } else {
                    this.client.setEndpoint(urlstr);
                }
            }
        }
    }

    /*
    Note to developer(s):
        When setting an endpoint for an AWS Client i.e. client.setEndpoint(endpointUrl),
        AWS Java SDK fails to parse the region correctly when the provided endpoint
        is an AWS PrivateLink so this method does the job of parsing the region name and
        returning it.

        Refer NIFI-5456 & NIFI-5893
     */
    private String parseRegionForVPCE(String url) {
        int index = url.length() - ".vpce.amazonaws.com".length();

        Pattern VPCE_ENDPOINT_PATTERN = Pattern.compile("^(?:.+[vpce-][a-z0-9-]+\\.)?([a-z0-9-]+)$");
        Matcher matcher = VPCE_ENDPOINT_PATTERN.matcher(url.substring(0, index));

        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            getLogger().warn("Unable to get a match with the VPCE endpoint pattern; defaulting the region to us-east-1...");
            return "us-east-1";
        }
    }

    /**
     * Create client from the arguments
     * @param context process context
     * @param credentials static aws credentials
     * @param config aws client configuration
     * @return ClientType aws client
     *
     * @deprecated use {@link AbstractAWSCredentialsProviderProcessor#createClient(ProcessContext, AWSCredentialsProvider, ClientConfiguration)}
     */
    @Deprecated
    protected abstract ClientType createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config);

    protected ClientType getClient() {
        return client;
    }

    protected Region getRegion() {
        return region;
    }

    protected AWSCredentials getCredentials(final ProcessContext context) {
        final String accessKey = context.getProperty(ACCESS_KEY).evaluateAttributeExpressions().getValue();
        final String secretKey = context.getProperty(SECRET_KEY).evaluateAttributeExpressions().getValue();

        final String credentialsFile = context.getProperty(CREDENTIALS_FILE).getValue();

        if (credentialsFile != null) {
            try {
                return new PropertiesCredentials(new File(credentialsFile));
            } catch (final IOException ioe) {
                throw new ProcessException("Could not read Credentials File", ioe);
            }
        }

        if (accessKey != null && secretKey != null) {
            return new BasicAWSCredentials(accessKey, secretKey);
        }

        return new AnonymousAWSCredentials();

    }

    @OnShutdown
    public void onShutdown() {
        if ( getClient() != null ) {
            getClient().shutdown();
        }
    }
}
