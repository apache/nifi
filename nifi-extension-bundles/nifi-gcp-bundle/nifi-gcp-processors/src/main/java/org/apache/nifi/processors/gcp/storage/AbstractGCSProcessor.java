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
package org.apache.nifi.processors.gcp.storage;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.BaseServiceException;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableMap;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.proxy.ProxyConfiguration;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for creating processors which connect to Google Cloud Storage.
 *
 * Every GCS processor operation requires a bucket, whether it's reading or writing from said bucket.
 */
public abstract class AbstractGCSProcessor extends AbstractGCPProcessor<Storage, StorageOptions> implements VerifiableProcessor {
    public static final Relationship REL_SUCCESS =
            new Relationship.Builder().name("success")
                    .description("FlowFiles are routed to this relationship after a successful Google Cloud Storage operation.")
                    .build();
    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description("FlowFiles are routed to this relationship if the Google Cloud Storage operation fails.")
                    .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    // https://cloud.google.com/storage/docs/request-endpoints#storage-set-client-endpoint-java
    public static final PropertyDescriptor STORAGE_API_URL = new PropertyDescriptor
            .Builder().name("storage-api-url")
            .displayName("Storage API URL")
            .description("Overrides the default storage URL. Configuring an alternative Storage API URL also overrides the "
                    + "HTTP Host header on requests as described in the Google documentation for Private Service Connections.")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .build();

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>(verifyCloudService(context, verificationLogger, attributes));
        final Storage storage = getCloudService(context);
        if (storage != null) {
            try {
                final String bucket = getBucketName(context, attributes);
                final List<String> requiredPermissions = getRequiredPermissions();
                if (storage.testIamPermissions(bucket, requiredPermissions).size() >= requiredPermissions.size()) {
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Test IAM Permissions")
                            .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                            .explanation(String.format("Verified Bucket [%s] exists and the configured user has the correct permissions.", bucket))
                            .build());
                } else {
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Test IAM Permissions")
                            .outcome(ConfigVerificationResult.Outcome.FAILED)
                            .explanation(String.format("The configured user does not have the correct permissions on Bucket [%s].", bucket))
                            .build());
                }
            } catch (final BaseServiceException e) {
                verificationLogger.error("The configured user appears to have the correct permissions, but the following error was encountered", e);
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Test IAM Permissions")
                        .outcome(ConfigVerificationResult.Outcome.FAILED)
                        .explanation(String.format("The configured user appears to have the correct permissions, but the following error was encountered: " + e.getMessage()))
                        .build());
            }
        }
        return results;
    }

    @Override
    protected final Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = super.customValidate(validationContext);
        ProxyConfiguration.validateProxySpec(validationContext, results, ProxyAwareTransportFactory.PROXY_SPECS);
        customValidate(validationContext, results);
        return results;
    }

    /**
     * If sub-classes needs to implement any custom validation, override this method then add validation result to the results.
     */
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
    }

    /**
     * @return The list of GCP permissions required for the processor
     */
    protected abstract List<String> getRequiredPermissions();

    protected String getBucketName(final ProcessContext context, final Map<String, String> attributes) {
        return context.getProperty("gcs-bucket").evaluateAttributeExpressions(attributes).getValue();
    }

    @Override
    protected StorageOptions getServiceOptions(ProcessContext context, GoogleCredentials credentials) {
        final String projectId = context.getProperty(PROJECT_ID).evaluateAttributeExpressions().getValue();
        final String storageApiUrl = context.getProperty(STORAGE_API_URL).evaluateAttributeExpressions().getValue();
        final Integer retryCount = context.getProperty(RETRY_COUNT).asInteger();

        StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder()
                .setCredentials(credentials)
                .setRetrySettings(RetrySettings.newBuilder()
                        .setMaxAttempts(retryCount)
                        .build());

        if (projectId != null && !projectId.isEmpty()) {
            storageOptionsBuilder.setProjectId(projectId);
        }

        if (storageApiUrl != null && !storageApiUrl.isEmpty()) {
            storageOptionsBuilder.setHost(storageApiUrl);
            // https://codelabs.developers.google.com/cloudnet-psc#12
            storageOptionsBuilder.setHeaderProvider(FixedHeaderProvider.create(ImmutableMap.of("Host", "www.googleapis.com")));
        }

        return  storageOptionsBuilder.setTransportOptions(getTransportOptions(context)).build();
    }

    protected String getTransitUri(final String storageApiUrl, final String bucketName, final String key) {
        final URI apiUri = URI.create(storageApiUrl);
        return String.format("%s://%s.%s/%s", apiUri.getScheme(), bucketName, apiUri.getHost(), key);
    }
}
