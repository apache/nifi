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
package org.apache.nifi.metrics.reporting.reporter.service;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.metrics.reporting.task.MetricsReportingTask;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A controller service that provides metric reporters for azure log analystics, can be used by {@link MetricsReportingTask}.
 *
 * @author Seokwon J. Yang
 */
@Tags({"metrics", "reporting", "azure", "loganalystics"})
@CapabilityDescription("A controller service that provides metric reporters for azure log analystics. " +
        "Used by MetricsReportingTask.")
public class AzureLogAnalysticsMetricReporterService extends AbstractControllerService implements MetricReporterService {

    /**
     * Azure Log Anaystics Workspace ID
     */
    public static final PropertyDescriptor AZ_WORKSPACE_ID = new PropertyDescriptor.Builder()
            .name("Azure Log Anaystics Workspace Id")
            .displayName("Azure Log Anaystics workspace id")
            .description("Azure Log Anaystics workspace id")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(true)
            .build();

    /**
     * Azure Log Anaystice Workspace Key
     */
    public static final PropertyDescriptor AZ_WORKSPACE_KEY = new PropertyDescriptor.Builder()
            .name("Azure Log Anaystics Workspace Key")
            .displayName("Azure Log Anaystics workspace key")
            .description("Azure Log Anaystics workspace key")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(true)
            .build();

    /**
     * LOG_TYPE name for custom logs collected thru Azure log analystics data collection API
     */
    public static final PropertyDescriptor AZ_WORKSPACE_LOG_TYPE = new PropertyDescriptor.Builder()
            .name("Azure Log Anaystics Workspace Log type")
            .displayName("Azure Log Anaystics Workspace Log type")
            .description("Azure Log Anaystics Workspace Log type. Custom logs map to ${LOG_TYPE}_CL")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    /**
     * List of property descriptors used by the service.
     */
    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(AZ_WORKSPACE_ID);
        props.add(AZ_WORKSPACE_KEY);
        props.add(AZ_WORKSPACE_LOG_TYPE);
        properties = Collections.unmodifiableList(props);
    }

    /**
     * ScheduledReporter implementation connected to Azure Log Analystics 
     */
    private ScheduledReporter azReporter;
    private String workspaceId;
    private String workspaceKey;
    private String logType;

    /**
     * Create the {@link #azReporter} according to configuration.
     *
     * @param context used to access properties.
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.workspaceId = context.getProperty(AZ_WORKSPACE_ID).evaluateAttributeExpressions().getValue();
        this.workspaceKey = context.getProperty(AZ_WORKSPACE_KEY).evaluateAttributeExpressions().getValue();
        this.logType = context.getProperty(AZ_WORKSPACE_LOG_TYPE).evaluateAttributeExpressions().getValue();
    }

    /**
     * Close the graphite sender.
     *
     * @throws IOException if failed to close the connection.
     */
    @OnDisabled
    public void shutdown() throws IOException {
        try {
            azReporter.close();
        } finally {
            azReporter = null;
        }
    }

    /**
     * Use the {@link #azReporter} in order to create a reporter.
     *
     * @param metricRegistry registry with the metrics to report.
     * @return a reporter instance.
     */
    @Override
    public ScheduledReporter createReporter(MetricRegistry metricRegistry) {
        this.azReporter = new AzureLogAnalysticsReporter(
            this.workspaceId,
            this.workspaceKey,
            this.logType,
            metricRegistry,
            MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);

        return this.azReporter;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
