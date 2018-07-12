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
package org.apache.nifi.pulsar;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;

public class StandardPulsarClientService extends AbstractControllerService implements PulsarClientService {

    public static final PropertyDescriptor PULSAR_SERVICE_URL = new PropertyDescriptor
            .Builder().name("PULSAR_SERVICE_URL")
            .displayName("Pulsar Service URL")
            .description("URL for the Pulsar cluster, e.g localhost:6650")
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor ALLOW_TLS_INSECURE_CONNECTION = new PropertyDescriptor.Builder()
            .name("Allow TLS insecure connection")
            .displayName("Allow TLS insecure connection")
            .description("Whether the Pulsar client will accept untrusted TLS certificate from broker or not.")
            .required(false)
            .allowableValues("true", "false")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor CONCURRENT_LOOKUP_REQUESTS = new PropertyDescriptor.Builder()
            .name("Maximum concurrent lookup-requests")
            .description("Number of concurrent lookup-requests allowed on each broker-connection.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("5000")
            .build();

    public static final PropertyDescriptor CONNECTIONS_PER_BROKER = new PropertyDescriptor.Builder()
            .name("Maximum connects per Pulsar broker")
            .description("Sets the max number of connection that the client library will open to a single broker.\n" +
                    "By default, the connection pool will use a single connection for all the producers and consumers. " +
                    "Increasing this parameter may improve throughput when using many producers over a high latency connection")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor IO_THREADS = new PropertyDescriptor.Builder()
            .name("I/O Threads")
            .description("The number of threads to be used for handling connections to brokers.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor KEEP_ALIVE_INTERVAL = new PropertyDescriptor.Builder()
            .name("Keep Alive interval")
            .displayName("Keep Alive interval")
            .description("The keep alive interval in seconds for each client-broker-connection.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("30 sec")
            .build();

    public static final PropertyDescriptor LISTENER_THREADS = new PropertyDescriptor.Builder()
            .name("Listener Threads")
            .description("The number of threads to be used for message listeners")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor MAXIMUM_LOOKUP_REQUESTS = new PropertyDescriptor.Builder()
            .name("Maximum lookup requests")
            .description("Number of max lookup-requests allowed on each broker-connection. To prevent overload on broker,"
                    + "it should be greater than the 'Maximum concurrent lookup-requests' property value.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("50000")
            .build();

    public static final PropertyDescriptor MAXIMUM_REJECTED_REQUESTS = new PropertyDescriptor.Builder()
            .name("Maximum rejected requests per connection")
            .description("Max number of broker-rejected requests in a certain time-frame after " +
                "which current connection will be closed and client creates a new connection that give " +
                "chance to connect a different broker")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("50")
            .build();

    public static final PropertyDescriptor OPERATION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Operation Timeout")
            .description("Producer-create, subscribe and unsubscribe operations will be retried until this " +
                "interval, after which the operation will be marked as failed")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("30 sec")
            .build();

    public static final PropertyDescriptor STATS_INTERVAL = new PropertyDescriptor.Builder()
            .name("Stats interval")
            .description("The interval between each stat infomation update. It should be set to at least 1 second")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("60 sec")
            .build();

    public static final PropertyDescriptor TLS_TRUST_CERTS_FILE_PATH = new PropertyDescriptor.Builder()
            .name("TLS Trust Certs File Path")
            .description("Set the path to the trusted TLS certificate file")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor USE_TCP_NO_DELAY = new PropertyDescriptor.Builder()
            .name("Use TCP no-delay flag")
            .description("Configure whether to use TCP no-delay flag on the connection, to disable Nagle algorithm.\n"
                    + "No-delay features make sure packets are sent out on the network as soon as possible, and it's critical "
                    + "to achieve low latency publishes. On the other hand, sending out a huge number of small packets might "
                    + "limit the overall throughput, so if latency is not a concern, it's advisable to set the useTcpNoDelay "
                    + "flag to false.")
            .required(false)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl.context.service")
            .displayName("SSL Context Service")
            .description("Specifies the SSL Context Service to use for communicating with Pulsar.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    private static List<PropertyDescriptor> properties;
    private volatile PulsarClient client;
    private boolean secure = false;
    private String pulsarBrokerRootUrl;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PULSAR_SERVICE_URL);
        props.add(ALLOW_TLS_INSECURE_CONNECTION);
        props.add(CONCURRENT_LOOKUP_REQUESTS);
        props.add(CONNECTIONS_PER_BROKER);
        props.add(IO_THREADS);
        props.add(KEEP_ALIVE_INTERVAL);
        props.add(LISTENER_THREADS);
        props.add(MAXIMUM_LOOKUP_REQUESTS);
        props.add(MAXIMUM_REJECTED_REQUESTS);
        props.add(OPERATION_TIMEOUT);
        props.add(STATS_INTERVAL);
        props.add(USE_TCP_NO_DELAY);
        props.add(SSL_CONTEXT_SERVICE);
        props.add(TLS_TRUST_CERTS_FILE_PATH);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
       return properties;
    }

    /**
     * @param context the configuration context
     * @throws InitializationException if unable to connect to the Pulsar Broker
     * @throws UnsupportedAuthenticationException if the Broker URL uses a non-supported authentication mechanism
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, UnsupportedAuthenticationException {
        try {
            client = getClientBuilder(context).build();
        } catch (Exception e) {
            throw new InitializationException("Unable to create Pulsar Client", e);
        }
    }

    @OnShutdown
    public void cleanup() throws PulsarClientException {
        if (client != null) {
          client.close();
        }
    }

    @Override
    public PulsarClient getPulsarClient() {
      return client;
    }

    @Override
    public String getPulsarBrokerRootURL() {
       return pulsarBrokerRootUrl;
    }

    private void setPulsarBrokerRootURL(String s) {
       pulsarBrokerRootUrl = s;
    }

    private static String buildPulsarBrokerRootUrl(String uri, boolean tlsEnabled) {
        StringBuilder builder = new StringBuilder().append("pulsar");

        if (tlsEnabled) {
           builder.append("+ssl");
        }

        return builder.append("://")
               .append(uri)
               .toString();
    }

    private ClientBuilder getClientBuilder(ConfigurationContext context) throws UnsupportedAuthenticationException, MalformedURLException {

        ClientBuilder builder = PulsarClient.builder()
             .allowTlsInsecureConnection(context.getProperty(ALLOW_TLS_INSECURE_CONNECTION).evaluateAttributeExpressions().asBoolean())
             .maxConcurrentLookupRequests(context.getProperty(CONCURRENT_LOOKUP_REQUESTS).asInteger())
             .connectionsPerBroker(context.getProperty(CONNECTIONS_PER_BROKER).asInteger())
             .ioThreads(context.getProperty(IO_THREADS).asInteger())
             .keepAliveInterval(context.getProperty(KEEP_ALIVE_INTERVAL).asTimePeriod(TimeUnit.SECONDS).intValue(), TimeUnit.SECONDS)
             .listenerThreads(context.getProperty(LISTENER_THREADS).asInteger())
             .maxLookupRequests(context.getProperty(MAXIMUM_LOOKUP_REQUESTS).asInteger())
             .maxNumberOfRejectedRequestPerConnection(context.getProperty(MAXIMUM_REJECTED_REQUESTS).asInteger())
             .operationTimeout(context.getProperty(OPERATION_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue(), TimeUnit.SECONDS)
             .statsInterval(context.getProperty(STATS_INTERVAL).asTimePeriod(TimeUnit.SECONDS).intValue(), TimeUnit.SECONDS)
             .enableTcpNoDelay(context.getProperty(USE_TCP_NO_DELAY).asBoolean());

        // Configure TLS
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService != null && sslContextService.isTrustStoreConfigured() && sslContextService.isKeyStoreConfigured()) {
           Map<String, String> authParams = new HashMap<>();
           authParams.put("tlsCertFile", sslContextService.getTrustStoreFile());
           authParams.put("tlsKeyFile", sslContextService.getKeyStoreFile());

           builder = builder.enableTls(true).authentication(AuthenticationTls.class.getName(), authParams)
                            .tlsTrustCertsFilePath(context.getProperty(TLS_TRUST_CERTS_FILE_PATH).evaluateAttributeExpressions().getValue());
           secure = true;
        }

        setPulsarBrokerRootURL(buildPulsarBrokerRootUrl(context.getProperty(PULSAR_SERVICE_URL).getValue(), secure));
        builder = builder.serviceUrl(getPulsarBrokerRootURL());
        return builder;
    }
}
