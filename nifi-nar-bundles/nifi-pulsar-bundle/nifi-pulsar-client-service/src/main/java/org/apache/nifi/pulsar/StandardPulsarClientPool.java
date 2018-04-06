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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.pulsar.pool.PulsarConsumerFactory;
import org.apache.nifi.pulsar.pool.PulsarProducerFactory;
import org.apache.nifi.pulsar.pool.ResourcePool;
import org.apache.nifi.pulsar.pool.ResourcePoolImpl;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.pulsar.client.api.ClientConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;

@Tags({ "Pulsar"})
@CapabilityDescription("Standard ControllerService implementation of PulsarClientService.")
public class StandardPulsarClientPool extends AbstractControllerService implements PulsarClientPool {

    public static final PropertyDescriptor PULSAR_SERVICE_URL = new PropertyDescriptor
            .Builder().name("PULSAR_SERVICE_URL")
            .displayName("Pulsar Service URL")
            .description("URL for the Pulsar cluster, e.g localhost:6650")
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONCURRENT_LOOKUP_REQUESTS = new PropertyDescriptor.Builder()
            .name("Maximum concurrent lookup-requests")
            .description("Number of concurrent lookup-requests allowed on each broker-connection to prevent "
                    + "overload on broker. (default: 5000) It should be configured with higher value only in case "
                    + "of it requires to produce/subscribe on thousands of topics")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5000")
            .build();

    public static final PropertyDescriptor CONNECTIONS_PER_BROKER = new PropertyDescriptor.Builder()
            .name("Maximum connects per Pulsar broker")
            .description("Sets the max number of connection that the client library will open to a single broker.\n" +
                    "By default, the connection pool will use a single connection for all the producers and consumers. " +
                    "Increasing this parameter may improve throughput when using many producers over a high latency connection")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor IO_THREADS = new PropertyDescriptor.Builder()
            .name("I/O Threads")
            .description("The number of threads to be used for handling connections to brokers (default: 1 thread)")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor LISTENER_THREADS = new PropertyDescriptor.Builder()
            .name("Listener Threads")
            .description("The number of threads to be used for message listeners (default: 1 thread)")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor MAXIMUM_REJECTED_REQUESTS = new PropertyDescriptor.Builder()
            .name("Maximum rejected requests per connection")
            .description("Max number of broker-rejected requests in a certain time-frame (30 seconds) after " +
                "which current connection will be closed and client creates a new connection that give " +
                "chance to connect a different broker (default: 50)")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("50")
            .build();

    public static final PropertyDescriptor OPERATION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Operation Timeout")
            .description("Producer-create, subscribe and unsubscribe operations will be retried until this " +
                "interval, after which the operation will be maked as failed (default: 30 seconds)")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("30")
            .build();

    public static final PropertyDescriptor STATS_INTERVAL = new PropertyDescriptor.Builder()
            .name("Stats interval")
            .description("The interval between each stat info (default: 60 seconds) Stats will be activated " +
                "with positive statsIntervalSeconds It should be set to at least 1 second")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("60")
            .build();

    public static final PropertyDescriptor USE_TCP_NO_DELAY = new PropertyDescriptor.Builder()
            .name("Use TCP nodelay flag")
            .description("Configure whether to use TCP no-delay flag on the connection, to disable Nagle algorithm.\n"
                    + "No-delay features make sure packets are sent out on the network as soon as possible, and it's critical "
                    + "to achieve low latency publishes. On the other hand, sending out a huge number of small packets might "
                    + "limit the overall throughput, so if latency is not a concern, it's advisable to set the useTcpNoDelay "
                    + "flag to false.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MAX_PRODUCERS = new PropertyDescriptor
            .Builder().name("MAX_PRODUCERS")
            .displayName("Producer Pool Size")
            .description("The Maximum Number of Pulsar Producers created by this Pulsar Client Pool")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_CONSUMERS = new PropertyDescriptor
            .Builder().name("MAX_CONSUMERS")
            .displayName("Consumer Pool Size")
            .description("The Maximum Number of Pulsar consumers created by this Pulsar Client Pool")
            .required(true)
            .defaultValue("10")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl.context.service")
            .displayName("SSL Context Service")
            .description("Specifies the SSL Context Service to use for communicating with Pulsar.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    private static final List<PropertyDescriptor> properties;
    private volatile PulsarClient client;

        private volatile ResourcePoolImpl<PulsarProducer> producers;
        private volatile ResourcePoolImpl<PulsarConsumer> consumers;
        private ClientConfiguration clientConfig;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PULSAR_SERVICE_URL);
        props.add(MAX_CONSUMERS);
        props.add(MAX_PRODUCERS);
        props.add(CONCURRENT_LOOKUP_REQUESTS);
        props.add(CONNECTIONS_PER_BROKER);
        props.add(IO_THREADS);
        props.add(LISTENER_THREADS);
        props.add(MAXIMUM_REJECTED_REQUESTS);
        props.add(OPERATION_TIMEOUT);
        props.add(STATS_INTERVAL);
        props.add(USE_TCP_NO_DELAY);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     * @throws UnsupportedAuthenticationException if the Broker URL uses a non-supported authentication mechanism
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException, UnsupportedAuthenticationException {

            createClient(context);

            if (this.client == null) {
                throw new InitializationException("Unable to create Pulsar Client");
            }

            producers = new ResourcePoolImpl<PulsarProducer>(new PulsarProducerFactory(client), context.getProperty(MAX_PRODUCERS).asInteger());
            consumers = new ResourcePoolImpl<PulsarConsumer>(new PulsarConsumerFactory(client,
                buildPulsarBrokerRootUrl(context.getProperty(PULSAR_SERVICE_URL).getValue(), getClientConfig(context).isUseTls())),
                context.getProperty(MAX_CONSUMERS).asInteger());

    }

    private void createClient(final ConfigurationContext context) throws InitializationException {

        try {
            this.client = PulsarClient.create(buildPulsarBrokerRootUrl(context.getProperty(PULSAR_SERVICE_URL).getValue(),
                        getClientConfig(context).isUseTls()), getClientConfig(context));

        } catch (Exception e) {
            throw new InitializationException("Unable to create Pulsar Client", e);
        }

    }

    private static String buildPulsarBrokerRootUrl(String uri, boolean tlsEnabled) {
        StringBuilder builder = new StringBuilder();
        builder.append("pulsar");

        if (tlsEnabled) {
           builder.append("+ssl");
        }

        builder.append("://");
        builder.append(uri);
        return builder.toString();
    }

    private ClientConfiguration getClientConfig(ConfigurationContext context) throws UnsupportedAuthenticationException {

        if (clientConfig == null) {
            clientConfig = new ClientConfiguration();

            if (context.getProperty(CONCURRENT_LOOKUP_REQUESTS).isSet()) {
                clientConfig.setConcurrentLookupRequest(context.getProperty(CONCURRENT_LOOKUP_REQUESTS).asInteger());
            }

            if (context.getProperty(CONNECTIONS_PER_BROKER).isSet()) {
                clientConfig.setConnectionsPerBroker(context.getProperty(CONNECTIONS_PER_BROKER).asInteger());
            }

            if (context.getProperty(IO_THREADS).isSet()) {
                clientConfig.setIoThreads(context.getProperty(IO_THREADS).asInteger());
            }

            if (context.getProperty(LISTENER_THREADS).isSet()) {
                clientConfig.setListenerThreads(context.getProperty(LISTENER_THREADS).asInteger());
            }

            if (context.getProperty(MAXIMUM_REJECTED_REQUESTS).isSet()) {
                clientConfig.setMaxNumberOfRejectedRequestPerConnection(context.getProperty(MAXIMUM_REJECTED_REQUESTS).asInteger());
            }

            if (context.getProperty(OPERATION_TIMEOUT).isSet()) {
                clientConfig.setOperationTimeout(context.getProperty(OPERATION_TIMEOUT).asInteger(), TimeUnit.SECONDS);
            }

            if (context.getProperty(STATS_INTERVAL).isSet()) {
                clientConfig.setStatsInterval(context.getProperty(STATS_INTERVAL).asLong(), TimeUnit.SECONDS);
            }

            if (context.getProperty(USE_TCP_NO_DELAY).isSet()) {
                clientConfig.setUseTcpNoDelay(context.getProperty(USE_TCP_NO_DELAY).asBoolean());
            }

            // Configure TLS
            final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

            if (sslContextService != null && sslContextService.isTrustStoreConfigured() && sslContextService.isKeyStoreConfigured()) {
                    clientConfig.setUseTls(true);
                    clientConfig.setTlsTrustCertsFilePath(sslContextService.getTrustStoreFile());

                    Map<String, String> authParams = new HashMap<>();

                    // TODO This should be a different value than the TlsTrustCertsFilePath above.
                    authParams.put("tlsCertFile", sslContextService.getTrustStoreFile());
                    authParams.put("tlsKeyFile", sslContextService.getKeyStoreFile());
                    clientConfig.setAuthentication(AuthenticationTls.class.getName(), authParams);
            }
        }

        return clientConfig;
    }

    @Override
    public ResourcePool<PulsarProducer> getProducerPool() {
        return this.producers;
    }

    @Override
    public ResourcePool<PulsarConsumer> getConsumerPool() {
        return this.consumers;
    }

}
