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
import org.apache.nifi.metrics.FlowMetricSet;
import org.apache.nifi.metrics.reporting.reporter.service.MetricReporterService;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ReportingInitializationContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A reporting task for NiFi instance and JVM related metrics.
 *
 * This task reports metrics to services according to a provided {@link ScheduledReporter}, reached by using a
 * {@link MetricReporterService}. In order to report to different clients, simply use different implementations of
 * the controller service.
 *
 * @see MetricReporterService
 * @author Omer Hadari
 */
@Tags({"metrics", "reporting"})
@CapabilityDescription("This reporting task reports a set of metrics regarding the JVM and the NiFi instance" +
        "to a reporter. The reporter is provided by a MetricReporterService.")
public class MetricsReportingTask extends AbstractReportingTask {

    /**
     * Points to the service which provides {@link ScheduledReporter} instances.
     */
    protected static final PropertyDescriptor REPORTER_SERVICE = new PropertyDescriptor.Builder()
            .name("metric reporter service")
            .displayName("metric reporter service")
            .description("The service that provides a reporter for the gathered metrics")
            .identifiesControllerService(MetricReporterService.class)
            .required(true)
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
     *
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
        currentStatusReference.set(context.getEventAccess().getControllerStatus());
        reporter.report();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(REPORTER_SERVICE);
        return Collections.unmodifiableList(properties);
    }
}
