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
import com.google.common.collect.ImmutableList;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.gcp.credentials.service.GCPCredentialsService;


import java.util.List;

/**
 * Abstract base class for gcp processors.
 *
 */
public abstract class AbstractGCPProcessor<
        CloudService extends Service<CloudServiceOptions>,
        CloudServiceOptions extends ServiceOptions<CloudService, CloudServiceOptions>> extends AbstractProcessor {

    public static final PropertyDescriptor PROJECT_ID = new PropertyDescriptor
            .Builder().name("gcp-project-id")
            .displayName("Project ID")
            .description("Google Cloud Project ID")
            .required(true)
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

    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor
            .Builder().name("gcp-proxy-host")
            .displayName("Proxy host")
            .description("IP or hostname of the proxy to be used")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor
            .Builder().name("gcp-proxy-port")
            .displayName("Proxy port")
            .description("Proxy port number")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();


    /**
     * Links to the {@link GCPCredentialsService} which provides credentials for this particular processor.
     */
    public static final PropertyDescriptor GCP_CREDENTIALS_PROVIDER_SERVICE = new PropertyDescriptor.Builder()
            .name("gcp-credentials-provider-service")
            .name("GCP Credentials Provider Service")
            .description("The Controller Service used to obtain Google Cloud Platform credentials.")
            .required(true)
            .identifiesControllerService(GCPCredentialsService.class)
            .build();


    protected volatile CloudService cloudService;

    protected CloudService getCloudService() {
        return cloudService;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(
                GCP_CREDENTIALS_PROVIDER_SERVICE,
                PROJECT_ID,
                RETRY_COUNT,
                PROXY_HOST,
                PROXY_PORT
        );
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
     * Assigns the cloud service client on scheduling.
     * @param context the process context provided on scheduling the processor.
     */
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        final CloudServiceOptions options = getServiceOptions(context, getGoogleCredentials(context));
        this.cloudService = options != null ? options.getService() : null;
    }

    /**
     * Builds the service-specific options as a necessary step in creating a cloud service.
     * @param context the process context provided on scheduling the processor.
     * @param credentials valid GoogleCredentials retrieved by the controller service.
     * @return CloudServiceOptions which can be initialized into a cloud service.
     * @see <a href="http://googlecloudplatform.github.io/google-cloud-java/0.8.0/apidocs/com/google/cloud/ServiceOptions.html">ServiceOptions</a>
     */
    protected abstract CloudServiceOptions getServiceOptions(ProcessContext context, GoogleCredentials credentials);
}
