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
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.services.solr.SolrClientService;
import org.apache.solr.client.solrj.SolrClient;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.nifi.processors.solr.SolrUtils.BASIC_PASSWORD;
import static org.apache.nifi.processors.solr.SolrUtils.BASIC_USERNAME;
import static org.apache.nifi.processors.solr.SolrUtils.CLIENT_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.KERBEROS_CREDENTIALS_SERVICE;
import static org.apache.nifi.processors.solr.SolrUtils.SOLR_LOCATION;
import static org.apache.nifi.processors.solr.SolrUtils.createKeytabUser;
import static org.apache.nifi.processors.solr.SolrUtils.validateConnectionDetails;

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
    private volatile boolean usedControllerService = false;

    protected volatile KerberosUser kerberosUser;

    @OnScheduled
    public final void onScheduled(final ProcessContext context) throws IOException {
        if (context.getProperty(CLIENT_SERVICE).isSet()) {
            SolrClientService client = context.getProperty(CLIENT_SERVICE).asControllerService(SolrClientService.class);
            solrClient = client.getClient();
            usedControllerService = true;
        } else {
            this.solrLocation = context.getProperty(SOLR_LOCATION).evaluateAttributeExpressions().getValue();
            this.basicUsername = context.getProperty(BASIC_USERNAME).evaluateAttributeExpressions().getValue();
            this.basicPassword = context.getProperty(BASIC_PASSWORD).evaluateAttributeExpressions().getValue();
            if (!StringUtils.isBlank(basicUsername) && !StringUtils.isBlank(basicPassword)) {
                basicAuthEnabled = true;
            }

            this.solrClient = createSolrClient(context, solrLocation);

            final KerberosCredentialsService kerberosCredentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
            if (kerberosCredentialsService != null && this.kerberosUser == null) {
                this.kerberosUser = createKeytabUser(kerberosCredentialsService);
            }

            usedControllerService = false;
        }
    }

    @OnStopped
    public final void closeClient() {
        if (!usedControllerService) {
            if (solrClient != null) {
                try {
                    solrClient.close();
                } catch (IOException e) {
                    getLogger().debug("Error closing SolrClient", e);
                }
            }

            if (kerberosUser != null) {
                try {
                    kerberosUser.logout();
                    kerberosUser = null;
                } catch (LoginException e) {
                    getLogger().debug("Error logging out keytab user", e);
                }
            }
        }
    }

    @Override
    public final void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final KerberosUser kerberosUser = getKerberosKeytabUser();
        if (kerberosUser == null) {
            doOnTrigger(context, session);
        } else {
            // wrap doOnTrigger in a privileged action
            final PrivilegedAction action = () -> {
                doOnTrigger(context, session);
                return null;
            };

            // execute the privileged action as the given keytab user
            final KerberosAction kerberosAction = new KerberosAction(kerberosUser, action, context, getLogger());
            kerberosAction.execute();
        }
    }

    /**
     * This should be implemented just like the normal onTrigger method. When a KerberosCredentialsService is configured,
     * this method will be wrapped in a PrivilegedAction and executed with the credentials of the service, otherwise this
     * will be executed like a a normal call to onTrigger.
     */
    protected abstract void doOnTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException;

    /**
     * Create a SolrClient based on the type of Solr specified.
     *
     * @param context
     *          The context
     * @return an HttpSolrClient or CloudSolrClient
     */
    protected SolrClient createSolrClient(final ProcessContext context, final String solrLocation) {
        return createSolrClient(context, solrLocation);
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

    protected final KerberosUser getKerberosKeytabUser() {
        return kerberosUser;
    }

    @Override
    final protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> problems = new ArrayList<>();

        List<ValidationResult> _temp = new ArrayList<>(validateConnectionDetails(context));
        boolean isClientSet = context.getProperty(CLIENT_SERVICE).isSet();
        boolean isLocationSet = context.getProperty(SOLR_LOCATION).isSet();

        if (isClientSet && isLocationSet) {
            _temp.add(new ValidationResult.Builder()
                .subject("Client Service/Solr location configuration")
                .explanation("Client service and Solr location cannot be set at the same time.").build());
        }

        if (_temp.size() == 0 && context.getProperty(CLIENT_SERVICE).isSet()) {
            return _temp; //This means that we're using the client service so we can stop.
        } else {
            problems.addAll(_temp);
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
