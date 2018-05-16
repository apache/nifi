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
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.nifi.processors.solr.SolrUtils.SOLR_TYPE_CLOUD;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_TYPE;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_TYPE_STANDARD;
import static org.apache.nifi.processors.solr.SolrUtils.COLLECTION;
import static org.apache.nifi.processors.solr.SolrUtils.JAAS_CLIENT_APP_NAME;
import static org.apache.nifi.processors.solr.SolrUtils.SSL_CONTEXT_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_LOCATION;
import static org.apache.nifi.processors.solr.SolrUtils.BASIC_USERNAME;
import static org.apache.nifi.processors.solr.SolrUtils.BASIC_PASSWORD;

/**
 * A base class for processors that interact with Apache Solr.
 *
 */
public abstract class SolrProcessor extends AbstractProcessor {

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
        return SolrUtils.createSolrClient(context, solrLocation);
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
    final protected Collection<ValidationResult> customValidate(ValidationContext context) {
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
