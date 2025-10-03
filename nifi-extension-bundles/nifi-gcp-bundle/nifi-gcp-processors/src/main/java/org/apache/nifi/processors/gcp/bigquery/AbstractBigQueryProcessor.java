/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.gcp.bigquery;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.BaseServiceException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.nifi.processors.gcp.util.GoogleUtils.GOOGLE_CLOUD_BIGQUERY_SCOPE;

/**
 * Base class for creating processors that connect to GCP BiqQuery service
 */
public abstract class AbstractBigQueryProcessor extends AbstractGCPProcessor<BigQuery, BigQueryOptions> implements VerifiableProcessor  {

    private static final List<String> REQUIRED_PERMISSIONS = Collections.singletonList("bigquery.tables.updateData");
    private static final String DATASET_ATTR = "Dataset";
    private static final String TABLE_NAME_ATTR = "Table Name";

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("FlowFiles are routed to this relationship after a successful Google BigQuery operation.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("FlowFiles are routed to this relationship if the Google BigQuery operation fails.")
            .build();

    public static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    public static final PropertyDescriptor DATASET = new PropertyDescriptor.Builder()
            .name(DATASET_ATTR)
            .description("BigQuery dataset name (Note - The dataset must exist in GCP)")
            .required(true)
            .defaultValue("${" + DATASET_ATTR + "}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name(TABLE_NAME_ATTR)
            .description("BigQuery table name")
            .required(true)
            .defaultValue("${" + TABLE_NAME_ATTR + "}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        super.migrateProperties(config);
        config.renameProperty("bq.dataset", DATASET.getName());
        config.renameProperty("bq.table.name", TABLE_NAME.getName());
    }

    @Override
    protected GoogleCredentials getGoogleCredentials(ProcessContext context) {
        return super.getGoogleCredentials(context).createScoped(GOOGLE_CLOUD_BIGQUERY_SCOPE);
    }

    @Override
    protected BigQueryOptions getServiceOptions(ProcessContext context, GoogleCredentials credentials) {
        final String projectId = context.getProperty(PROJECT_ID).evaluateAttributeExpressions().getValue();
        final Integer retryCount = Integer.valueOf(context.getProperty(RETRY_COUNT).getValue());

        final BigQueryOptions.Builder builder = BigQueryOptions.newBuilder();

        if (!StringUtils.isBlank(projectId)) {
            builder.setProjectId(projectId);
        }

        return builder.setCredentials(credentials)
                .setRetrySettings(RetrySettings.newBuilder().setMaxAttempts(retryCount).build())
                .setTransportOptions(getTransportOptions(context))
                .build();
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>(verifyCloudService(context, verificationLogger, attributes));

        final BigQuery bigQuery = getCloudService(context);
        if (bigQuery != null) {
            try {
                final TableId tableId = getTableId(context, attributes);
                if (bigQuery.testIamPermissions(tableId, REQUIRED_PERMISSIONS).size() >= REQUIRED_PERMISSIONS.size()) {
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Test IAM Permissions")
                            .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                            .explanation(String.format("Verified BigQuery Table [%s] exists and the configured user has the correct permissions.", tableId))
                            .build());
                } else {
                    results.add(new ConfigVerificationResult.Builder()
                            .verificationStepName("Test IAM Permissions")
                            .outcome(ConfigVerificationResult.Outcome.FAILED)
                            .explanation(String.format("The configured user does not have the correct permissions on BigQuery Table [%s].", tableId))
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
        final Collection<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        ProxyConfiguration.validateProxySpec(validationContext, results, ProxyAwareTransportFactory.PROXY_SPECS);

        final boolean projectId = validationContext.getProperty(PROJECT_ID).isSet();
        if (!projectId) {
            results.add(new ValidationResult.Builder()
                    .subject(PROJECT_ID.getName())
                    .valid(false)
                    .explanation("The Project ID must be set for this processor.")
                    .build());
        }

        customValidate(validationContext, results);
        return results;
    }

    /**
     * If subclasses needs to implement any custom validation, override this method then add
     * validation result to the results.
     */
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
    }

    protected TableId getTableId(final ProcessContext context, final Map<String, String> attributes) {
        final String projectId = context.getProperty(PROJECT_ID).evaluateAttributeExpressions().getValue();
        final String dataset = context.getProperty(DATASET).evaluateAttributeExpressions(attributes).getValue();
        final String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(attributes).getValue();

        final TableId tableId;
        if (StringUtils.isEmpty(projectId)) {
            tableId = TableId.of(dataset, tableName);
        } else {
            tableId = TableId.of(projectId, dataset, tableName);
        }
        return tableId;
    }

}
