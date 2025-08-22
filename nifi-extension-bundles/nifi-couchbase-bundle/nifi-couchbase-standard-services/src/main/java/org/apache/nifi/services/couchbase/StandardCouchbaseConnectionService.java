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
package org.apache.nifi.services.couchbase;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;

import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.ReplicateTo;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.ssl.StandardKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerFactoryBuilder;
import org.apache.nifi.services.couchbase.utils.CouchbaseContext;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.util.List;

@CapabilityDescription("Provides a standard Couchbase connection service implementation.")
@Tags({"nosql", "couchbase", "database", "connection"})
public class StandardCouchbaseConnectionService extends AbstractControllerService implements CouchbaseConnectionService {

    public static final PropertyDescriptor CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("Connection String")
            .description("The hostnames or ip addresses of the bootstraping nodes and optional parameters."
                    + " Syntax: couchbase://node1,node2,nodeN?param1=value1&param2=value2&paramN=valueN")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username to authenticate to the Couchbase client.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The user's password to authenticate to the Couchbase client.")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("Service supporting SSL communication configuration.")
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor PERSIST_TO = new PropertyDescriptor.Builder()
            .name("Persist To")
            .description("Durability constraint about disk persistence.")
            .required(true)
            .allowableValues(PersistTo.values())
            .defaultValue(PersistTo.NONE.toString())
            .build();

    public static final PropertyDescriptor REPLICATE_TO = new PropertyDescriptor.Builder()
            .name("Replicate To")
            .description("Durability constraint about replication.")
            .required(true)
            .allowableValues(ReplicateTo.values())
            .defaultValue(ReplicateTo.NONE.toString())
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = List.of(
            CONNECTION_STRING, USERNAME, PASSWORD, SSL_CONTEXT_SERVICE, PERSIST_TO, REPLICATE_TO);

    private volatile Cluster cluster;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    private String connectionString;
    private PersistTo persistTo;
    private ReplicateTo replicateTo;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        persistTo = PersistTo.valueOf(context.getProperty(PERSIST_TO).getValue());
        replicateTo = ReplicateTo.valueOf(context.getProperty(REPLICATE_TO).getValue());
        connectionString = context.getProperty(CONNECTION_STRING).evaluateAttributeExpressions().getValue();

        ClusterOptions clusterOptions = ClusterOptions.clusterOptions(username, password);
        final PropertyValue sslContextServiceProperty = context.getProperty(SSL_CONTEXT_SERVICE);
        if (sslContextServiceProperty.isSet()) {
            final SSLContextService sslContextService = sslContextServiceProperty.asControllerService(SSLContextService.class);

            final KeyStore trustStore;
            try (InputStream inputStream = new FileInputStream(sslContextService.getTrustStoreFile())) {
                trustStore = new StandardKeyStoreBuilder()
                        .type(sslContextService.getTrustStoreType())
                        .password(sslContextService.getTrustStorePassword().toCharArray())
                        .inputStream(inputStream)
                        .build();
            } catch (final IOException e) {
                throw new InitializationException("Trust Store loading failed", e);
            }

            final TrustManagerFactory trustManagerFactory = new StandardTrustManagerFactoryBuilder().trustStore(trustStore).build();
            final ClusterEnvironment environment = ClusterEnvironment.builder()
                    .securityConfig(security -> security
                            .enableTls(true)
                            .trustManagerFactory(trustManagerFactory))
                    .build();

            clusterOptions = clusterOptions.environment(environment);
        }

        cluster = Cluster.connect(connectionString, clusterOptions);
    }

    @OnDisabled
    public void onDisabled() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Override
    public String getServiceLocation() {
        return connectionString;
    }

    @Override
    public CouchbaseClient getClient(CouchbaseContext context) {
        final Collection collection = cluster
                .bucket(context.bucket())
                .scope(context.scope())
                .collection(context.collection());

        return new StandardCouchbaseClient(collection, context.documentType(), persistTo, replicateTo);
    }
}
