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
package org.apache.nifi.metrics.reporting.task;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.metrics.FlowMetricSet;
import org.apache.nifi.metrics.reporting.reporter.service.MetricReporterService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A reporting task for NiFi instance and JVM related metrics.
 * <p>
 * This task reports metrics to services according to a provided {@link ScheduledReporter}, reached by using a
 * {@link MetricReporterService}. In order to report to different clients, simply use different implementations of
 * the controller service.
 *
 * @author Omer Hadari
 * @see MetricReporterService
 */
@Tags({"metrics", "reporting"})
@CapabilityDescription("This reporting task reports a set of metrics regarding the JVM and the NiFi instance" +
        "to a reporter. The reporter is provided by a MetricReporterService. It can be optionally used for a specific" +
        "process group if a property with the group id is provided.")
public class MetricsReportingTask extends AbstractReportingTask {

    /**
     * Points to the service which provides {@link ScheduledReporter} instances.
     */
    protected static final PropertyDescriptor REPORTER_SERVICE = new PropertyDescriptor.Builder()
            .name("metric reporter service")
            .displayName("Metric Reporter Service")
            .description("The service that provides a reporter for the gathered metrics")
            .identifiesControllerService(MetricReporterService.class)
            .required(true)
            .build();

    /**
     * Metrics of the process group with this ID should be reported. If not specified, use the root process group.
     */
    protected static final PropertyDescriptor PROCESS_GROUP_ID = new PropertyDescriptor.Builder()
            .name("process group id")
            .displayName("Process Group ID")
            .description("The id of the process group to report. If not specified, metrics of the root process group" +
                    "are reported.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * Contains the metrics that should be reported.
     */
    private MetricRegistry metricRegistry;

    /**
     * Used for actually reporting metrics.
     */
    private ScheduledReporter reporter;

    // Protected for testing sake. DO NOT ACCESS FOR OTHER PURPOSES.
    /**
     * Points to the most recent process group status seen by this task.
     */
    protected AtomicReference<ProcessGroupStatus> currentStatusReference;

    /**
     * Register all wanted metrics to {@link #metricRegistry}.
     * <p>
     * {@inheritDoc}
     */
    @Override
    protected void init(ReportingInitializationContext config) {
        metricRegistry = new MetricRegistry();
        currentStatusReference = new AtomicReference<>();
        metricRegistry.registerAll(new MemoryUsageGaugeSet());
        metricRegistry.registerAll(new FlowMetricSet(currentStatusReference));
    }

    /**
     * Populate {@link #reporter} using the {@link MetricReporterService}. If the reporter is active already,
     * do nothing.
     *
     * @param context used for accessing the controller service.
     */
    @OnScheduled
    public void connect(ConfigurationContext context) {
        if (reporter == null) {
            reporter = ((MetricReporterService) context.getProperty(REPORTER_SERVICE).asControllerService())
                    .createReporter(metricRegistry);
        }
    }

    /**
     * Report the registered metrics.
     *
     * @param context used for getting the most recent {@link ProcessGroupStatus}.
     */
    @Override
    public void onTrigger(ReportingContext context) {
        String groupId = context.getProperty(PROCESS_GROUP_ID).evaluateAttributeExpressions().getValue();

        ProcessGroupStatus statusToReport = groupId == null
                ? context.getEventAccess().getControllerStatus()
                : context.getEventAccess().getGroupStatus(groupId);

        if (statusToReport != null) {
            currentStatusReference.set(statusToReport);
            reporter.report();
        } else {
            getLogger().error("Process group with provided group id could not be found.");
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(REPORTER_SERVICE);
        properties.add(PROCESS_GROUP_ID);
        return Collections.unmodifiableList(properties);
    }
}
