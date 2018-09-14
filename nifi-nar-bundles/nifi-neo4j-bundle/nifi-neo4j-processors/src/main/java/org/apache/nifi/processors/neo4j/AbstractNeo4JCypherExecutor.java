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
package org.apache.nifi.processors.neo4j;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Config.ConfigBuilder;
import org.neo4j.driver.v1.Config.LoadBalancingStrategy;
import org.neo4j.driver.v1.Config.TrustStrategy;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

/**
 * Abstract base class for Neo4JCypherExecutor processors
 */
abstract class AbstractNeo4JCypherExecutor extends AbstractProcessor {

    protected static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
            .name("neo4J-query")
            .displayName("Neo4J Query")
            .description("Specifies the Neo4j Query.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

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
            .description("Password for Neo4J user")
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static AllowableValue LOAD_BALANCING_STRATEGY_ROUND_ROBIN = new AllowableValue(LoadBalancingStrategy.ROUND_ROBIN.name(), "Round Robin", "Round Robin Strategy");

    public static AllowableValue LOAD_BALANCING_STRATEGY_LEAST_CONNECTED = new AllowableValue(LoadBalancingStrategy.LEAST_CONNECTED.name(), "Least Connected", "Least Connected Strategy");

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
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-pool-size")
            .displayName("Neo4J Max Connection Pool Size")
            .description("The maximum connection pool size for Neo4j.")
            .defaultValue("100")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_ACQUISITION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-acquisition-timeout")
            .displayName("Neo4J Max Connection Acquisition Timeout")
            .description("The maximum connection acquisition timeout.")
            .defaultValue("60 second")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor IDLE_TIME_BEFORE_CONNECTION_TEST = new PropertyDescriptor.Builder()
            .name("neo4j-idle-time-before-test")
            .displayName("Neo4J Idle Time Before Connection Test")
            .description("The idle time before connection test.")
            .defaultValue("60 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_CONNECTION_LIFETIME = new PropertyDescriptor.Builder()
            .name("neo4j-max-connection-lifetime")
            .displayName("Neo4J Max Connection Lifetime")
            .description("The maximum connection lifetime")
            .defaultValue("3600 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor ENCRYPTION = new PropertyDescriptor.Builder()
            .name("neo4j-encryption")
            .displayName("Neo4J Encrytion")
            .description("Is connection encrypted")
            .defaultValue("true")
            .required(true)
            .allowableValues("true","false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .sensitive(false)
            .build();

    public static AllowableValue TRUST_SYSTEM_CA_SIGNED_CERTIFICATES =
        new AllowableValue(TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES.name(),
            "Trust System CA Signed Certificates", "Trust system specified CA signed certificates");

    public static AllowableValue TRUST_CUSTOM_CA_SIGNED_CERTIFICATES =
        new AllowableValue(TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES.name(),
            "Trust Custom CA Signed Certificates", "Trust custom CA signed certificates defined in the file");

    public static AllowableValue TRUST_ALL_CERTIFICATES =
        new AllowableValue(TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES.name(),
            "Trust All Certificates", "Trust all certificate");

    protected static final PropertyDescriptor TRUST_STRATEGY = new PropertyDescriptor.Builder()
            .name("neo4j-trust-strategy")
            .displayName("Trust Strategy")
            .description("Trust Strategy (Trust All Certificates, System CA Signed Certificates or Custom CA Signed Certificates)")
            .required(false)
            .defaultValue(TRUST_ALL_CERTIFICATES.getValue())
            .allowableValues(TRUST_ALL_CERTIFICATES, TRUST_SYSTEM_CA_SIGNED_CERTIFICATES, TRUST_CUSTOM_CA_SIGNED_CERTIFICATES)
            .build();

    protected static final PropertyDescriptor TRUST_CUSTOM_CA_SIGNED_CERTIFICATES_FILE = new PropertyDescriptor.Builder()
            .name("neo4j-custom-ca-strategy-certificates-file")
            .displayName("Custom Trust CA Signed Certificates File")
            .description("Custom file containing CA signed certificates to be trusted.")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Sucessful FlowFiles are routed to this relationship").build();

    static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed FlowFiles are routed to this relationship").build();

    public static final String ERROR_MESSAGE = "neo4j.error.message";
    public static final String NODES_CREATED= "neo4j.nodes.created";
    public static final String RELATIONS_CREATED = "neo4j.relations.created";
    public static final String LABELS_ADDED = "neo4j.labels.added";
    public static final String NODES_DELETED = "neo4j.nodes.deleted";
    public static final String RELATIONS_DELETED = "neo4j.relations.deleted";
    public static final String PROPERTIES_SET = "neo4j.properties.set";
    public static final String ROWS_RETURNED = "neo4j.rows.returned";

    protected Driver neo4JDriver;
    protected String username;
    protected String password;
    protected String connectionUrl;
    protected Integer port;
    protected LoadBalancingStrategy loadBalancingStrategy;

    /**
     * Helper method to help testability
     * @return Driver instance
     */
    protected Driver getNeo4JDriver() {
        return neo4JDriver;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            neo4JDriver = getDriver(context);
        } catch(Exception e) {
            getLogger().error("Error while getting connection " + e.getLocalizedMessage(),e);
            throw new ProcessException("Error while getting connection" + e.getLocalizedMessage(),e);
        }
        getLogger().info("Neo4JCypherExecutor connection created for url {}",
                new Object[] {connectionUrl});
    }

    protected Driver getDriver(ProcessContext context) {
        connectionUrl = context.getProperty(CONNECTION_URL).evaluateAttributeExpressions().getValue();
        username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

        ConfigBuilder configBuilder = Config.build();
        String loadBalancingStrategyValue = context.getProperty(LOAD_BALANCING_STRATEGY).getValue();
        if ( ! StringUtils.isBlank(loadBalancingStrategyValue) ) {
             configBuilder = configBuilder.withLoadBalancingStrategy(
                     LoadBalancingStrategy.valueOf(loadBalancingStrategyValue));
        }

        configBuilder.withMaxConnectionPoolSize(context.getProperty(MAX_CONNECTION_POOL_SIZE).asInteger());

        configBuilder.withConnectionAcquisitionTimeout(context.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        configBuilder.withMaxConnectionLifetime(context.getProperty(MAX_CONNECTION_ACQUISITION_TIMEOUT).asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        configBuilder.withConnectionLivenessCheckTimeout(context.getProperty(IDLE_TIME_BEFORE_CONNECTION_TEST).asTimePeriod(TimeUnit.SECONDS), TimeUnit.SECONDS);

        if ( context.getProperty(ENCRYPTION).asBoolean() ) {
            configBuilder.withEncryption();
        } else {
            configBuilder.withoutEncryption();
        }

        PropertyValue trustStrategy = context.getProperty(TRUST_STRATEGY);
        if ( trustStrategy.isSet() ) {
            if ( trustStrategy.getValue().equals(TRUST_CUSTOM_CA_SIGNED_CERTIFICATES.getValue())) {
                configBuilder.withTrustStrategy(TrustStrategy.trustCustomCertificateSignedBy(new File(
                    context.getProperty(TRUST_CUSTOM_CA_SIGNED_CERTIFICATES_FILE).getValue())));
            } else if ( trustStrategy.getValue().equals(TRUST_SYSTEM_CA_SIGNED_CERTIFICATES.getValue())) {
                configBuilder.withTrustStrategy(TrustStrategy.trustSystemCertificates());
            } else if ( trustStrategy.getValue().equals(TRUST_ALL_CERTIFICATES.getValue())) {
                configBuilder.withTrustStrategy(TrustStrategy.trustAllCertificates());
            }
        }

        return GraphDatabase.driver( connectionUrl, AuthTokens.basic( username, password),
                 configBuilder.toConfig());
    }

    @OnStopped
    public void close() {
        getLogger().info("Closing driver");
        if ( neo4JDriver != null ) {
            neo4JDriver.close();
            neo4JDriver = null;
        }
    }
}