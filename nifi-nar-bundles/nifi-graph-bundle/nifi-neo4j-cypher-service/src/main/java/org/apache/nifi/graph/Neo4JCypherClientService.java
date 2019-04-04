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

package org.apache.nifi.graph;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.neo4j.driver.internal.InternalNode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.SummaryCounters;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Tags({ "graph", "neo4j", "cypher" })
@CapabilityDescription("Provides a client service for managing connections to a Neo4J database. Configuration information for " +
        "the Neo4J driver that corresponds to most of the settings for this service can be found here: " +
        "https://neo4j.com/docs/driver-manual/current/client-applications/#driver-configuration")
public class Neo4JCypherClientService extends AbstractControllerService implements GraphClientService {
    public static final PropertyDescriptor CONNECTION_URL = new PropertyDescriptor.Builder()
            .name("neo4j-connection-url")
            .displayName("Neo4j Connection URL")
            .description("Neo4J endpoing to connect to.")
            .required(true)
            .defaultValue("bolt://localhost:7687")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("neo4j-username")
            .displayName("Username")
            .description("Username for accessing Neo4J")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("neo4j-password")
            .displayName("Password")
            .description("Password for Neo4J user. A dummy non-blank password is required even if it disabled on the server.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static AllowableValue LOAD_BALANCING_STRATEGY_ROUND_ROBIN = new AllowableValue(Config.LoadBalancingStrategy.ROUND_ROBIN.name(), "Round Robin", "Round Robin Strategy");

    public static AllowableValue LOAD_BALANCING_STRATEGY_LEAST_CONNECTED = new AllowableValue(Config.LoadBalancingStrategy.LEAST_CONNECTED.name(), "Least Connected", "Least Connected Strategy");

    protected static final PropertyDescriptor LOAD_BALANCING_STRATEGY = new PropertyDescriptor.Builder()
            .name("neo4j-load-balancing-strategy")
            .displayName("Load Balancing Strategy")
            .description("Load Balancing Strategy (Round Robin or Least Connected)")
            .required(false)
            .defaultValue(LOAD_BALANCING_STRATEGY_ROUND_ROBIN.getValue())
            .allowableValues(LOAD_BALANCING_STRATEGY_ROUND_ROBIN, LOAD_BALANCING_STRATEGY_LEAST_CONNECTED)
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-time-out")
            .displayName("Neo4J Max Connection Time Out (seconds)")
            .description("The maximum time for establishing connection to the Neo4j")
            .defaultValue("5 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-pool-size")
            .displayName("Neo4J Max Connection Pool Size")
            .description("The maximum connection pool size for Neo4j.")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_ACQUISITION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-acquisition-timeout")
            .displayName("Neo4J Max Connection Acquisition Timeout")
            .description("The maximum connection acquisition timeout.")
            .defaultValue("60 second")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor IDLE_TIME_BEFORE_CONNECTION_TEST = new PropertyDescriptor.Builder()
            .name("neo4j-idle-time-before-test")
            .displayName("Neo4J Idle Time Before Connection Test")
            .description("The idle time before connection test.")
            .defaultValue("60 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_LIFETIME = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-lifetime")
            .displayName("Neo4J Max Connection Lifetime")
            .description("The maximum connection lifetime")
            .defaultValue("3600 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor ENCRYPTION = new PropertyDescriptor.Builder()
            .name("neo4j-driver-tls-encryption-enabled")
            .displayName("Neo4J Driver TLS Encryption")
            .description("Is the driver using TLS encryption ?")
            .defaultValue("true")
            .required(true)
            .allowableValues("true","false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL "
                    + "connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    protected Driver neo4JDriver;
    protected String username;
    protected String password;
    protected String connectionUrl;

    private static final List<PropertyDescriptor> DESCRIPTORS;
    static {
        List<PropertyDescriptor> _temp = new ArrayList<>();
        _temp.add(CONNECTION_URL);
        _temp.add(USERNAME);
        _temp.add(PASSWORD);
        _temp.add(LOAD_BALANCING_STRATEGY);
        _temp.add(CONNECTION_TIMEOUT);
        _temp.add(MAX_CONNECTION_POOL_SIZE);
        _temp.add(MAX_CONNECTION_ACQUISITION_TIMEOUT);
        _temp.add(IDLE_TIME_BEFORE_CONNECTION_TEST);
        _temp.add(MAX_CONNECTION_LIFETIME);
        _temp.add(ENCRYPTION);
        _temp.add(SSL_CONTEXT_SERVICE);

        DESCRIPTORS = Collections.unmodifiableList(_temp);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    protected Driver getDriver(ConfigurationContext context) {
        connectionUrl = context.getProperty(CONNECTION_URL).evaluateAttributeExpressions().getValue();
        username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        password = context.getProperty(PASSWORD).getValue();

        Config.ConfigBuilder configBuilder = Config.build();
        String loadBalancingStrategyValue = context.getProperty(LOAD_BALANCING_STRATEGY).getValue();
        if ( ! StringUtils.isBlank(loadBalancingStrategyValue) ) {
            configBuilder = configBuilder.withLoadBalancingStrategy(
                    Config.LoadBalancingStrategy.valueOf(loadBalancingStrategyValue));
        }

        configBuilder.withMaxConnectionPoolSize(context.getProperty(MAX_CONNECTION_POOL_SIZE).evaluateAttributeExpressions().asInteger());

        configBuilder.withConnectionTimeout(context.getProperty(CONNECTION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        configBuilder.withConnectionAcquisitionTimeout(context.getProperty(MAX_CONNECTION_ACQUISITION_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        configBuilder.withMaxConnectionLifetime(context.getProperty(MAX_CONNECTION_LIFETIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        configBuilder.withConnectionLivenessCheckTimeout(context.getProperty(IDLE_TIME_BEFORE_CONNECTION_TEST).evaluateAttributeExpressions().asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        if ( context.getProperty(ENCRYPTION).asBoolean() ) {
            configBuilder.withEncryption();
        } else {
            configBuilder.withoutEncryption();
        }

        final SSLContextService sslService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslService != null) {
            if ( sslService.isTrustStoreConfigured()) {
                configBuilder.withTrustStrategy(Config.TrustStrategy.trustCustomCertificateSignedBy(new File(
                        sslService.getTrustStoreFile())));
            } else {
                configBuilder.withTrustStrategy(Config.TrustStrategy.trustSystemCertificates());
            }
        }

        return GraphDatabase.driver( connectionUrl, AuthTokens.basic( username, password),
                configBuilder.toConfig());
    }

    /**
     * Helper method to help testability
     * @return Driver instance
     */
    protected Driver getNeo4JDriver() {
        return neo4JDriver;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        try {
            neo4JDriver = getDriver(context);
        } catch(Exception e) {
            getLogger().error("Error while getting connection " + e.getLocalizedMessage(),e);
            throw new ProcessException("Error while getting connection" + e.getLocalizedMessage(),e);
        }
        getLogger().info("Neo4JCypherExecutor connection created for url {}",
                new Object[] {connectionUrl});
    }

    @OnDisabled
    public void close() {
        getLogger().info("Closing driver");
        if ( neo4JDriver != null ) {
            neo4JDriver.close();
            neo4JDriver = null;
        }
    }

    private Map<String, Object> handleInternalNode(Map<String, Object> recordMap) {
        if (recordMap.size() == 1) {
            String key = recordMap.keySet().iterator().next();
            Object value = recordMap.get(key);
            if (value instanceof InternalNode) {
                return ((InternalNode)value).asMap();
            }
        }

        return recordMap;
    }

    @Override
    public Map<String, String> executeQuery(String query, Map<String, Object> parameters, GraphQueryResultCallback handler) {
        try (Session session = neo4JDriver.session()) {
            StatementResult result = session.run(query, parameters);
            long count = 0;
            while (result.hasNext()) {
                Record record = result.next();
                Map<String, Object> asMap = handleInternalNode(record.asMap());
                handler.process(asMap, result.hasNext());
                count++;
            }

            ResultSummary summary = result.summary();
            SummaryCounters counters = summary.counters();

            Map<String,String> resultAttributes = new HashMap<>();
            resultAttributes.put(NODES_CREATED,String.valueOf(counters.nodesCreated()));
            resultAttributes.put(RELATIONS_CREATED,String.valueOf(counters.relationshipsCreated()));
            resultAttributes.put(LABELS_ADDED,String.valueOf(counters.labelsAdded()));
            resultAttributes.put(NODES_DELETED,String.valueOf(counters.nodesDeleted()));
            resultAttributes.put(RELATIONS_DELETED,String.valueOf(counters.relationshipsDeleted()));
            resultAttributes.put(PROPERTIES_SET, String.valueOf(counters.propertiesSet()));
            resultAttributes.put(ROWS_RETURNED, String.valueOf(count));

            return resultAttributes;
        } catch (Exception ex) {
            getLogger().error("", ex);
            throw new ProcessException(ex);
        }
    }

    @Override
    public String getTransitUrl() {
        return null;
    }
}
