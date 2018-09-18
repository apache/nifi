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

package org.apache.nifi.processors.gcp.bigquery;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.common.collect.ImmutableList;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base class for creating processors that connect to GCP BiqQuery service
 */
public abstract class AbstractBigQueryProcessor extends AbstractGCPProcessor<BigQuery, BigQueryOptions> {

    static final int BUFFER_SIZE = 65536;

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder().name("success")
                    .description("FlowFiles are routed to this relationship after a successful Google BigQuery operation.")
                    .build();
    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description("FlowFiles are routed to this relationship if the Google BigQuery operation fails.")
                    .build();

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    public static final PropertyDescriptor DATASET = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.DATASET_ATTR)
            .displayName("Dataset")
            .description(BigQueryAttributes.DATASET_DESC)
            .required(true)
            .defaultValue("${" + BigQueryAttributes.DATASET_ATTR + "}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.TABLE_NAME_ATTR)
            .displayName("Table Name")
            .description(BigQueryAttributes.TABLE_NAME_DESC)
            .required(true)
            .defaultValue("${" + BigQueryAttributes.TABLE_NAME_ATTR + "}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_SCHEMA = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.TABLE_SCHEMA_ATTR)
            .displayName("Table Schema")
            .description(BigQueryAttributes.TABLE_SCHEMA_DESC)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.JOB_READ_TIMEOUT_ATTR)
            .displayName("Read Timeout")
            .description(BigQueryAttributes.JOB_READ_TIMEOUT_DESC)
            .required(true)
            .defaultValue("5 minutes")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.<PropertyDescriptor>builder()
                .addAll(super.getSupportedPropertyDescriptors())
                .add(DATASET)
                .add(TABLE_NAME)
                .add(TABLE_SCHEMA)
                .add(READ_TIMEOUT)
                .build();
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
    protected final Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = super.customValidate(validationContext);
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
     * If sub-classes needs to implement any custom validation, override this method then add validation result to the results.
     */
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
    }

}
