/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.solr;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.common.params.ModifiableSolrParams;

import javax.net.ssl.SSLContext;
import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A base class for processors that interact with Apache Solr.
 *
 */
public abstract class SolrProcessor extends AbstractProcessor {

    public static final AllowableValue SOLR_TYPE_CLOUD = new AllowableValue(
            "Cloud", "Cloud", "A SolrCloud instance.");

    public static final AllowableValue SOLR_TYPE_STANDARD = new AllowableValue(
            "Standard", "Standard", "A stand-alone Solr instance.");

    public static final PropertyDescriptor SOLR_TYPE = new PropertyDescriptor
            .Builder().name("Solr Type")
            .description("The type of Solr instance, Cloud or Standard.")
            .required(true)
            .allowableValues(SOLR_TYPE_CLOUD, SOLR_TYPE_STANDARD)
            .defaultValue(SOLR_TYPE_STANDARD.getValue())
            .build();

    public static final PropertyDescriptor SOLR_LOCATION = new PropertyDescriptor
            .Builder().name("Solr Location")
            .description("The Solr url for a Solr Type of Standard (ex: http://localhost:8984/solr/gettingstarted), " +
                    "or the ZooKeeper hosts for a Solr Type of Cloud (ex: localhost:9983).")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor COLLECTION = new PropertyDescriptor
            .Builder().name("Collection")
            .description("The Solr collection name, only used with a Solr Type of Cloud")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor JAAS_CLIENT_APP_NAME = new PropertyDescriptor
            .Builder().name("JAAS Client App Name")
            .description("The name of the JAAS configuration entry to use when performing Kerberos authentication to Solr. If this property is " +
                    "not provided, Kerberos authentication will not be attempted. The value must match an entry in the file specified by the " +
                    "system property java.security.auth.login.config.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor BASIC_USERNAME = new PropertyDescriptor
            .Builder().name("Username")
            .description("The username to use when Solr is configured with basic authentication.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor BASIC_PASSWORD = new PropertyDescriptor
            .Builder().name("Password")
            .description("The password to use when Solr is configured with basic authentication.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
            .expressionLanguageSupported(true)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context. This property must be set when communicating with a Solr over https.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    public static final PropertyDescriptor SOLR_SOCKET_TIMEOUT = new PropertyDescriptor
            .Builder().name("Solr Socket Timeout")
            .description("The amount of time to wait for data on a socket connection to Solr. A value of 0 indicates an infinite timeout.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .build();

    public static final PropertyDescriptor SOLR_CONNECTION_TIMEOUT = new PropertyDescriptor
            .Builder().name("Solr Connection Timeout")
            .description("The amount of time to wait when establishing a connection to Solr. A value of 0 indicates an infinite timeout.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("10 seconds")
            .build();

    public static final PropertyDescriptor SOLR_MAX_CONNECTIONS = new PropertyDescriptor
            .Builder().name("Solr Maximum Connections")
            .description("The maximum number of total connections allowed from the Solr client to Solr.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();

    public static final PropertyDescriptor SOLR_MAX_CONNECTIONS_PER_HOST = new PropertyDescriptor
            .Builder().name("Solr Maximum Connections Per Host")
            .description("The maximum number of connections allowed from the Solr client to a single Solr host.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("5")
            .build();

    public static final PropertyDescriptor ZK_CLIENT_TIMEOUT = new PropertyDescriptor
            .Builder().name("ZooKeeper Client Timeout")
            .description("The amount of time to wait for data on a connection to ZooKeeper, only used with a Solr Type of Cloud.")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .defaultValue("10 seconds")
            .build();

    public static final PropertyDescriptor ZK_CONNECTION_TIMEOUT = new PropertyDescriptor
            .Builder().name("ZooKeeper Connection Timeout")
            .description("The amount of time to wait when establishing a connection to ZooKeeper, only used with a Solr Type of Cloud.")
            .required(false)
            .addValidator(StandardValidators.createTimePeriodValidator(1, TimeUnit.SECONDS, Integer.MAX_VALUE, TimeUnit.SECONDS))
            .defaultValue("10 seconds")
            .build();

    private volatile SolrClient solrClient;
    private volatile String solrLocation;
    private volatile String basicUsername;
    private volatile String basicPassword;
    private volatile boolean basicAuthEnabled = false;

    @OnScheduled
    public final void onScheduled(final ProcessContext context) throws IOException {
        this.solrLocation =  context.getProperty(SOLR_LOCATION).evaluateAttributeExpressions().getValue();
        this.basicUsername = context.getProperty(BASIC_USERNAME).evaluateAttributeExpressions().getValue();
        this.basicPassword = context.getProperty(BASIC_PASSWORD).evaluateAttributeExpressions().getValue();
        if (!StringUtils.isBlank(basicUsername) && !StringUtils.isBlank(basicPassword)) {
            basicAuthEnabled = true;
        }
        this.solrClient = createSolrClient(context, solrLocation);
    }

    @OnStopped
    public final void closeClient() {
        if (solrClient != null) {
            try {
                solrClient.close();
            } catch (IOException e) {
                getLogger().debug("Error closing SolrClient", e);
            }
        }
    }

    /**
     * Create a SolrClient based on the type of Solr specified.
     *
     * @param context
     *          The context
     * @return an HttpSolrClient or CloudSolrClient
     */
    protected SolrClient createSolrClient(final ProcessContext context, final String solrLocation) {
        final Integer socketTimeout = context.getProperty(SOLR_SOCKET_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer connectionTimeout = context.getProperty(SOLR_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final Integer maxConnections = context.getProperty(SOLR_MAX_CONNECTIONS).asInteger();
        final Integer maxConnectionsPerHost = context.getProperty(SOLR_MAX_CONNECTIONS_PER_HOST).asInteger();
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final String jaasClientAppName = context.getProperty(JAAS_CLIENT_APP_NAME).getValue();

        final ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(HttpClientUtil.PROP_SO_TIMEOUT, socketTimeout);
        params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, connectionTimeout);
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, maxConnections);
        params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);

        // has to happen before the client is created below so that correct configurer would be set if neeeded
        if (!StringUtils.isEmpty(jaasClientAppName)) {
            System.setProperty("solr.kerberos.jaas.appname", jaasClientAppName);
            HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
        }

        final HttpClient httpClient = HttpClientUtil.createClient(params);

        if (sslContextService != null) {
            final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.REQUIRED);
            final SSLSocketFactory sslSocketFactory = new SSLSocketFactory(sslContext);
            final Scheme httpsScheme = new Scheme("https", 443, sslSocketFactory);
            httpClient.getConnectionManager().getSchemeRegistry().register(httpsScheme);
        }

        if (SOLR_TYPE_STANDARD.equals(context.getProperty(SOLR_TYPE).getValue())) {
            return new HttpSolrClient(solrLocation, httpClient);
        } else {
            final String collection = context.getProperty(COLLECTION).evaluateAttributeExpressions().getValue();
            final Integer zkClientTimeout = context.getProperty(ZK_CLIENT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
            final Integer zkConnectionTimeout = context.getProperty(ZK_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();

            CloudSolrClient cloudSolrClient = new CloudSolrClient(solrLocation, httpClient);
            cloudSolrClient.setDefaultCollection(collection);
            cloudSolrClient.setZkClientTimeout(zkClientTimeout);
            cloudSolrClient.setZkConnectTimeout(zkConnectionTimeout);
            return cloudSolrClient;
        }
    }

    /**
     * Returns the {@link org.apache.solr.client.solrj.SolrClient} that was created by the
     * {@link #createSolrClient(org.apache.nifi.processor.ProcessContext, String)} method
     *
     * @return an HttpSolrClient or CloudSolrClient
     */
    protected final SolrClient getSolrClient() {
        return solrClient;
    }

    protected final String getSolrLocation() {
        return solrLocation;
    }

    protected final String getUsername() {
        return basicUsername;
    }

    protected final String getPassword() {
        return basicPassword;
    }

    protected final boolean isBasicAuthEnabled() {
        return basicAuthEnabled;
    }

    @Override
    protected final Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>();

        if (SOLR_TYPE_CLOUD.equals(context.getProperty(SOLR_TYPE).getValue())) {
            final String collection = context.getProperty(COLLECTION).getValue();
            if (collection == null || collection.trim().isEmpty()) {
                problems.add(new ValidationResult.Builder()
                        .subject(COLLECTION.getName())
                        .input(collection).valid(false)
                        .explanation("A collection must specified for Solr Type of Cloud")
                        .build());
            }
        }

        // If a JAAS Client App Name is provided then the system property for the JAAS config file must be set,
        // and that config file must contain an entry for the name provided by the processor
        final String jaasAppName = context.getProperty(JAAS_CLIENT_APP_NAME).getValue();
        if (!StringUtils.isEmpty(jaasAppName)) {
            final String loginConf = System.getProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP);
            if (StringUtils.isEmpty(loginConf)) {
                problems.add(new ValidationResult.Builder()
                        .subject(JAAS_CLIENT_APP_NAME.getDisplayName())
                        .valid(false)
                        .explanation("the system property " + Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP + " must be set when providing a JAAS Client App Name")
                        .build());
            } else {
                final Configuration config = javax.security.auth.login.Configuration.getConfiguration();
                if (config.getAppConfigurationEntry(jaasAppName) == null) {
                    problems.add(new ValidationResult.Builder()
                            .subject(JAAS_CLIENT_APP_NAME.getDisplayName())
                            .valid(false)
                            .explanation("'" + jaasAppName + "' does not exist in " + loginConf)
                            .build());
                }
            }
        }

        // For solr cloud the location will be the ZooKeeper host:port so we can't validate the SSLContext, but for standard solr
        // we can validate if the url starts with https we need an SSLContextService, if it starts with http we can't have an SSLContextService
        if (SOLR_TYPE_STANDARD.equals(context.getProperty(SOLR_TYPE).getValue())) {
            final String solrLocation = context.getProperty(SOLR_LOCATION).evaluateAttributeExpressions().getValue();
            if (solrLocation != null) {
                final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
                if (solrLocation.startsWith("https:") && sslContextService == null) {
                    problems.add(new ValidationResult.Builder()
                            .subject(SSL_CONTEXT_SERVICE.getDisplayName())
                            .valid(false)
                            .explanation("an SSLContextService must be provided when using https")
                            .build());
                } else if (solrLocation.startsWith("http:") && sslContextService != null) {
                    problems.add(new ValidationResult.Builder()
                            .subject(SSL_CONTEXT_SERVICE.getDisplayName())
                            .valid(false)
                            .explanation("an SSLContextService can not be provided when using http")
                            .build());
                }
            }
        }

        // Validate that we username and password are provided together, or that neither are provided
        final String username = context.getProperty(BASIC_USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(BASIC_PASSWORD).evaluateAttributeExpressions().getValue();

        if (!StringUtils.isBlank(username) && StringUtils.isBlank(password)) {
            problems.add(new ValidationResult.Builder()
                    .subject(BASIC_PASSWORD.getDisplayName())
                    .valid(false)
                    .explanation("a password must be provided for the given username")
                    .build());
        }

        if (!StringUtils.isBlank(password) && StringUtils.isBlank(username)) {
            problems.add(new ValidationResult.Builder()
                    .subject(BASIC_USERNAME.getDisplayName())
                    .valid(false)
                    .explanation("a username must be provided for the given password")
                    .build());
        }

        Collection<ValidationResult> otherProblems = this.additionalCustomValidation(context);
        if (otherProblems != null) {
            problems.addAll(otherProblems);
        }

        return problems;
    }

    /**
     * Allows additional custom validation to be done. This will be called from
     * the parent's customValidation method.
     *
     * @param context
     *            The context
     * @return Validation results indicating problems
     */
    protected Collection<ValidationResult> additionalCustomValidation(ValidationContext context) {
        return new ArrayList<>();
    }

}
