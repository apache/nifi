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
package org.apache.nifi.processors.gcp;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Service;
import com.google.cloud.ServiceOptions;
import com.google.cloud.TransportOptions;
import com.google.cloud.http.HttpTransportOptions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.ProxyServiceMigration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;

/**
 * Abstract base class for gcp processors.
 *
 */
public abstract class AbstractGCPProcessor<
        CloudService extends Service<CloudServiceOptions>,
        CloudServiceOptions extends ServiceOptions<CloudService, CloudServiceOptions>> extends AbstractProcessor {

    // Obsolete property names
    private static final String OBSOLETE_PROXY_HOST = "gcp-proxy-host";
    private static final String OBSOLETE_PROXY_PORT = "gcp-proxy-port";
    private static final String OBSOLETE_PROXY_USERNAME = "gcp-proxy-user-name";
    private static final String OBSOLETE_PROXY_PASSWORD = "gcp-proxy-user-password";

    public static final PropertyDescriptor PROJECT_ID = new PropertyDescriptor
            .Builder().name("gcp-project-id")
            .displayName("Project ID")
            .description("Google Cloud Project ID")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RETRY_COUNT = new PropertyDescriptor
            .Builder().name("gcp-retry-count")
            .displayName("Number of retries")
            .description("How many retry attempts should be made before routing to the failure relationship.")
            .defaultValue("6")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor GCP_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
        .name("GCP Credentials Provider Service")
        .description("The Controller Service used to obtain Google Cloud Platform credentials.")
        .required(true)
        .identifiesControllerService(GCPCredentialsService.class)
        .build();

    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration.createProxyConfigPropertyDescriptor(false, ProxyAwareTransportFactory.PROXY_SPECS);

    protected volatile CloudService cloudService;

    protected CloudService getCloudService() {
        return cloudService;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(PROJECT_ID,
                GCP_CREDENTIALS_PROVIDER_SERVICE,
                RETRY_COUNT,
                PROXY_CONFIGURATION_SERVICE
        );
    }


    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        ProxyServiceMigration.migrateProxyProperties(config, PROXY_CONFIGURATION_SERVICE, OBSOLETE_PROXY_HOST, OBSOLETE_PROXY_PORT, OBSOLETE_PROXY_USERNAME, OBSOLETE_PROXY_PASSWORD);
    }

    /**
     * Verifies the cloud service configuration.  This is in a separate method rather than implementing VerifiableProcessor due to type erasure.
     * @param context The process context
     * @param verificationLogger Logger for verification
     * @param attributes Additional attributes
     * @return The verification results
     */
    protected List<ConfigVerificationResult> verifyCloudService(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {

        ConfigVerificationResult result = null;
        try {
            final CloudService cloudService = getCloudService(context);
            if (cloudService != null) {
                result = new ConfigVerificationResult.Builder()
                        .verificationStepName("Configure Cloud Service")
                        .outcome(Outcome.SUCCESSFUL)
                        .explanation(String.format("Successfully configured Cloud Service [%s]", cloudService.getClass().getSimpleName()))
                        .build();
            }
        } catch (final Exception e) {
            verificationLogger.error("Failed to configure Cloud Service", e);
            result = new ConfigVerificationResult.Builder()
                    .verificationStepName("Configure Cloud Service")
                    .outcome(Outcome.FAILED)
                    .explanation(String.format("Failed to configure Cloud Service [%s]: %s", cloudService.getClass().getSimpleName(), e.getMessage()))
                    .build();
        }
        return result == null ? Collections.emptyList() : Collections.singletonList(result);
    }

    /**
     * Retrieve credentials from the {@link GCPCredentialsService} attached to this processor.
     * @param context the process context provided on scheduling the processor.
     * @return GoogleCredentials for the processor to access.
     * @see  <a href="https://developers.google.com/api-client-library/java/google-api-java-client/reference/1.20.0/com/google/api/client/googleapis/auth/oauth2/GoogleCredential">AuthCredentials</a>
     */
    protected GoogleCredentials getGoogleCredentials(final ProcessContext context) {
        final GCPCredentialsService gcpCredentialsService =
                context.getProperty(GCP_CREDENTIALS_PROVIDER_SERVICE).asControllerService(GCPCredentialsService.class);
        return gcpCredentialsService.getGoogleCredentials();
    }

    /**
     * Returns the cloud client service constructed based on the context.
     * @param context the process context
     * @return The constructed cloud client service
     */
    protected CloudService getCloudService(final ProcessContext context) {
        final CloudServiceOptions options = getServiceOptions(context, getGoogleCredentials(context));
        return options != null ? options.getService() : null;
    }

    /**
     * Assigns the cloud service client on scheduling.
     * @param context the process context provided on scheduling the processor.
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.cloudService = getCloudService(context);
    }

    /**
     * Builds the service-specific options as a necessary step in creating a cloud service.
     * @param context the process context provided on scheduling the processor.
     * @param credentials valid GoogleCredentials retrieved by the controller service.
     * @return CloudServiceOptions which can be initialized into a cloud service.
     * @see <a href="http://googlecloudplatform.github.io/google-cloud-java/0.8.0/apidocs/com/google/cloud/ServiceOptions.html">ServiceOptions</a>
     */
    protected abstract CloudServiceOptions getServiceOptions(ProcessContext context, GoogleCredentials credentials);

    /**
     * Builds the Transport Options containing the proxy configuration
     * @param context Context to get properties
     * @return Transport options object with proxy configuration
     */
    protected TransportOptions getTransportOptions(ProcessContext context) {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context);

        final ProxyAwareTransportFactory transportFactory = new ProxyAwareTransportFactory(proxyConfiguration);
        return HttpTransportOptions.newBuilder().setHttpTransportFactory(transportFactory).build();
    }
}
