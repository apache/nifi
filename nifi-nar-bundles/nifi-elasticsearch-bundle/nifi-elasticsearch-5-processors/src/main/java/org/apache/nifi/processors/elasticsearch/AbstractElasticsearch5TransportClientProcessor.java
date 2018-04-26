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
package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;


abstract class AbstractElasticsearch5TransportClientProcessor extends AbstractElasticsearch5Processor {

    protected static final PropertyDescriptor CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("el5-cluster-name")
            .displayName("Cluster Name")
            .description("Name of the ES cluster (for example, elasticsearch_brew). Defaults to 'elasticsearch'")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("elasticsearch")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor HOSTS = new PropertyDescriptor.Builder()
            .name("el5-hosts")
            .displayName("ElasticSearch Hosts")
            .description("ElasticSearch Hosts, which should be comma separated and colon for hostname/port "
                    + "host1:port,host2:port,....  For example testcluster:9300. This processor uses the Transport Client to "
                    + "connect to hosts. The default transport client port is 9300.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_XPACK_LOCATION = new PropertyDescriptor.Builder()
            .name("el5-xpack-location")
            .displayName("X-Pack Transport Location")
            .description("Specifies the path to the JAR(s) for the Elasticsearch X-Pack Transport feature. "
                    + "If the Elasticsearch cluster has been secured with the X-Pack plugin, then the X-Pack Transport "
                    + "JARs must also be available to this processor. Note: Do NOT place the X-Pack JARs into NiFi's "
                    + "lib/ directory, doing so will prevent the X-Pack Transport JARs from being loaded.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dynamicallyModifiesClasspath(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor PING_TIMEOUT = new PropertyDescriptor.Builder()
            .name("el5-ping-timeout")
            .displayName("ElasticSearch Ping Timeout")
            .description("The ping timeout used to determine when a node is unreachable. " +
                    "For example, 5s (5 seconds). If non-local recommended is 30s")
            .required(true)
            .defaultValue("5s")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected static final PropertyDescriptor SAMPLER_INTERVAL = new PropertyDescriptor.Builder()
            .name("el5-sampler-interval")
            .displayName("Sampler Interval")
            .description("How often to sample / ping the nodes listed and connected. For example, 5s (5 seconds). "
                    + "If non-local recommended is 30s.")
            .required(true)
            .defaultValue("5s")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected final AtomicReference<Client> esClient = new AtomicReference<>();
    protected List<InetSocketAddress> esHosts;

    /**
     * Instantiate ElasticSearch Client. This should be called by subclasses' @OnScheduled method to create a client
     * if one does not yet exist. If called when scheduled, closeClient() should be called by the subclasses' @OnStopped
     * method so the client will be destroyed when the processor is stopped.
     *
     * @param context The context for this processor
     * @throws ProcessException if an error occurs while creating an Elasticsearch client
     */
    @Override
    protected void createElasticsearchClient(ProcessContext context) throws ProcessException {

        ComponentLog log = getLogger();
        if (esClient.get() != null) {
            return;
        }

        log.debug("Creating ElasticSearch Client");
        try {
            final String clusterName = context.getProperty(CLUSTER_NAME).evaluateAttributeExpressions().getValue();
            final String pingTimeout = context.getProperty(PING_TIMEOUT).evaluateAttributeExpressions().getValue();
            final String samplerInterval = context.getProperty(SAMPLER_INTERVAL).evaluateAttributeExpressions().getValue();
            final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
            final String password = context.getProperty(PASSWORD).getValue();

            final SSLContextService sslService =
                    context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

            Settings.Builder settingsBuilder = Settings.builder()
                    .put("cluster.name", clusterName)
                    .put("client.transport.ping_timeout", pingTimeout)
                    .put("client.transport.nodes_sampler_interval", samplerInterval);

            String xPackUrl = context.getProperty(PROP_XPACK_LOCATION).evaluateAttributeExpressions().getValue();
            if (sslService != null) {
                settingsBuilder.put("xpack.security.transport.ssl.enabled", "true");
                if (!StringUtils.isEmpty(sslService.getKeyStoreFile())) {
                    settingsBuilder.put("xpack.ssl.keystore.path", sslService.getKeyStoreFile());
                }
                if (!StringUtils.isEmpty(sslService.getKeyStorePassword())) {
                    settingsBuilder.put("xpack.ssl.keystore.password", sslService.getKeyStorePassword());
                }
                if (!StringUtils.isEmpty(sslService.getKeyPassword())) {
                    settingsBuilder.put("xpack.ssl.keystore.key_password", sslService.getKeyPassword());
                }
                if (!StringUtils.isEmpty(sslService.getTrustStoreFile())) {
                    settingsBuilder.put("xpack.ssl.truststore.path", sslService.getTrustStoreFile());
                }
                if (!StringUtils.isEmpty(sslService.getTrustStorePassword())) {
                    settingsBuilder.put("xpack.ssl.truststore.password", sslService.getTrustStorePassword());
                }
            }

            // Set username and password for X-Pack
            if (!StringUtils.isEmpty(username)) {
                StringBuffer secureUser = new StringBuffer(username);
                if (!StringUtils.isEmpty(password)) {
                    secureUser.append(":");
                    secureUser.append(password);
                }
                settingsBuilder.put("xpack.security.user", secureUser);
            }

            final String hosts = context.getProperty(HOSTS).evaluateAttributeExpressions().getValue();
            esHosts = getEsHosts(hosts);
            Client transportClient = getTransportClient(settingsBuilder, xPackUrl, username, password, esHosts, log);
            esClient.set(transportClient);

        } catch (Exception e) {
            log.error("Failed to create Elasticsearch client due to {}", new Object[]{e}, e);
            throw new ProcessException(e);
        }
    }

    protected Client getTransportClient(Settings.Builder settingsBuilder, String xPackPath,
                                        String username, String password,
                                        List<InetSocketAddress> esHosts, ComponentLog log)
            throws MalformedURLException {

        // Map of headers
        Map<String, String> headers = new HashMap<>();

        TransportClient transportClient = null;

        // See if the Elasticsearch X-Pack JAR locations were specified, and create the
        // authorization token if username and password are supplied.
        if (!StringUtils.isBlank(xPackPath)) {
            ClassLoader xPackClassloader = Thread.currentThread().getContextClassLoader();
            try {
                // Get the plugin class
                Class xPackTransportClientClass = Class.forName("org.elasticsearch.xpack.client.PreBuiltXPackTransportClient", true, xPackClassloader);
                Constructor<?> ctor = xPackTransportClientClass.getConstructor(Settings.class, Class[].class);

                if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {

                    // Need a couple of classes from the X-Path Transport JAR to build the token
                    Class usernamePasswordTokenClass =
                            Class.forName("org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken", true, xPackClassloader);

                    Class securedStringClass =
                            Class.forName("org.elasticsearch.xpack.security.authc.support.SecuredString", true, xPackClassloader);

                    Constructor<?> securedStringCtor = securedStringClass.getConstructor(char[].class);
                    Object securePasswordString = securedStringCtor.newInstance(password.toCharArray());

                    Method basicAuthHeaderValue = usernamePasswordTokenClass.getMethod("basicAuthHeaderValue", String.class, securedStringClass);
                    String authToken = (String) basicAuthHeaderValue.invoke(null, username, securePasswordString);
                    if (authToken != null) {
                        headers.put("Authorization", authToken);
                    }
                    transportClient = (TransportClient) ctor.newInstance(settingsBuilder.build(), new Class[0]);
                }
            } catch (ClassNotFoundException
                    | NoSuchMethodException
                    | InstantiationException
                    | IllegalAccessException
                    | InvocationTargetException xPackLoadException) {
                throw new ProcessException("X-Pack plugin could not be loaded and/or configured", xPackLoadException);
            }
        } else {
            getLogger().debug("No X-Pack Transport location specified, secure connections and/or authorization will not be available");
        }
        // If transportClient is null, either the processor is not configured for secure connections or there is a problem with config
        // (which is logged), so continue with a non-secure client
        if (transportClient == null) {
            transportClient = new PreBuiltTransportClient(settingsBuilder.build());
        }
        if (esHosts != null) {
            for (final InetSocketAddress host : esHosts) {
                try {
                    transportClient.addTransportAddress(new InetSocketTransportAddress(host));
                } catch (IllegalArgumentException iae) {
                    log.error("Could not add transport address {}", new Object[]{host});
                }
            }
        }

        Client client = transportClient.filterWithHeader(headers);
        return client;
    }

    /**
     * Dispose of ElasticSearch client
     */
    public void closeClient() {
        Client client = esClient.get();
        if (client != null) {
            getLogger().info("Closing ElasticSearch Client");
            esClient.set(null);
            client.close();
        }
    }

    /**
     * Get the ElasticSearch hosts from a NiFi attribute, e.g.
     *
     * @param hosts A comma-separated list of ElasticSearch hosts (host:port,host2:port2, etc.)
     * @return List of InetSocketAddresses for the ES hosts
     */
    private List<InetSocketAddress> getEsHosts(String hosts) {

        if (hosts == null) {
            return null;
        }
        final List<String> esList = Arrays.asList(hosts.split(","));
        List<InetSocketAddress> esHosts = new ArrayList<>();

        for (String item : esList) {

            String[] addresses = item.split(":");
            final String hostName = addresses[0].trim();
            final int port = Integer.parseInt(addresses[1].trim());

            esHosts.add(new InetSocketAddress(hostName, port));
        }
        return esHosts;
    }


}
