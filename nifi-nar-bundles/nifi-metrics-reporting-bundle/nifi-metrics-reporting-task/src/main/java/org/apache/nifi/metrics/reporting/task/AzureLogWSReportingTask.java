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

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.metrics.jvm.JvmMetrics;
import org.apache.nifi.metrics.reporting.reporter.service.AzureLogWSReporter;
import org.apache.nifi.metrics.reporting.reporter.service.MetricReporterService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.util.metrics.MetricsService;
import org.apache.nifi.scheduling.SchedulingStrategy;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObjectBuilder;
import javax.json.JsonWriter;

import com.codahale.metrics.MetricRegistry;

import java.io.IOException;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Tags({"reporting", "azure", "metrics"})
@CapabilityDescription("Publishes metrics from NiFi to Azure Log Analystics." +
        "Configure this reporting task with AzureWSMetricReporterService.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class AzureLogWSReportingTask extends AbstractReportingTask {

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

    protected static final PropertyDescriptor PROCESS_GROUP_ID = new PropertyDescriptor.Builder()
            .name("Process Group ID")
            .description("If specified, the reporting task will send metrics about this process group only. If"
                    + " not, the root process group is used and global metrics are sent.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    /**
     * Used for actually reporting metrics.
     */
    private AzureLogWSReporter reporter;

    private volatile JvmMetrics virtualMachineMetrics;

    private final MetricsService metricsService = new MetricsService();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(REPORTER_SERVICE);
        properties.add(PROCESS_GROUP_ID);
        return properties;
    }

    @OnScheduled
    public void setup(final ConfigurationContext context) throws IOException {
        virtualMachineMetrics = JmxJvmMetrics.getInstance();
        if (reporter == null) {
            reporter = (AzureLogWSReporter) ((MetricReporterService) context.getProperty(REPORTER_SERVICE).asControllerService())
                    .createReporter(new MetricRegistry());
        }
        
    }


    @Override
    public void onTrigger(final ReportingContext context) {

        final boolean pgIdIsSet = context.getProperty(PROCESS_GROUP_ID).isSet();
        final String processGroupId = pgIdIsSet ? context.getProperty(PROCESS_GROUP_ID).evaluateAttributeExpressions().getValue() : null;

        final ProcessGroupStatus status = processGroupId == null ? context.getEventAccess().getControllerStatus() : context.getEventAccess().getGroupStatus(processGroupId);

        if(status != null) {
            final Map<String,String> statusMetrics = metricsService.getMetrics(status, false);
            final Map<String,String> jvmMetrics = metricsService.getMetrics(virtualMachineMetrics);
            JsonArrayBuilder jarrayBuilder = Json.createArrayBuilder();
            JsonObjectBuilder jobjBuilder = Json.createObjectBuilder();

            String reportingHost;
            try {
                reportingHost = InetAddress.getLocalHost().getHostName();
                jobjBuilder.add("Computer", reportingHost);
            } catch (UnknownHostException e) {
                getLogger().error("Error in getting reporting host name: {}", e.getStackTrace());
                return;
			}
            

            for (Map.Entry<String,String> entry : statusMetrics.entrySet()) {
                jobjBuilder.add(entry.getKey(), entry.getValue());
            }
            for (Map.Entry<String,String> entry : jvmMetrics.entrySet()) {
                jobjBuilder.add(entry.getKey(), entry.getValue());
            }
            JsonArray metricsObject = jarrayBuilder.add(jobjBuilder).build();

            StringWriter sWriter = new StringWriter();

            try (JsonWriter jsonWriter = Json.createWriter(sWriter)){
                jsonWriter.writeArray(metricsObject);
            }
            String jdata = sWriter.toString();
            getLogger().debug(String.format("jdata to send to Azure log analystics: %s", jdata));

            reporter.sendToAzureLogAnalystics(jdata);


        } else {
            getLogger().error("No process group status with ID = {}", new Object[]{processGroupId});
        }
    }

}
