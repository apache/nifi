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
package org.apache.nifi.reporting.ambari;

import com.yammer.metrics.core.VirtualMachineMetrics;

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.ambari.api.MetricsBuilder;
import org.apache.nifi.reporting.ambari.metrics.MetricsService;
import org.apache.nifi.scheduling.SchedulingStrategy;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Tags({"reporting", "ambari", "metrics"})
@CapabilityDescription("Publishes metrics from NiFi to Ambari Metrics Service (AMS). Due to how the Ambari Metrics Service " +
        "works, this reporting task should be scheduled to run every 60 seconds. Each iteration it will send the metrics " +
        "from the previous iteration, and calculate the current metrics to be sent on next iteration. Scheduling this reporting " +
        "task at a frequency other than 60 seconds may produce unexpected results.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class AmbariReportingTask extends AbstractReportingTask {

    static final PropertyDescriptor METRICS_COLLECTOR_URL = new PropertyDescriptor.Builder()
            .name("Metrics Collector URL")
            .description("The URL of the Ambari Metrics Collector Service")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("http://localhost:6188/ws/v1/timeline/metrics")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    static final PropertyDescriptor APPLICATION_ID = new PropertyDescriptor.Builder()
            .name("Application ID")
            .description("The Application ID to be included in the metrics sent to Ambari")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The Hostname of this NiFi instance to be included in the metrics sent to Ambari")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("${hostname(true)}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor PROCESS_GROUP_ID = new PropertyDescriptor.Builder()
            .name("Process Group ID")
            .description("If specified, the reporting task will send metrics about this process group only. If"
                    + " not, the root process group is used and global metrics are sent.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private volatile Client client;
    private volatile JsonBuilderFactory factory;
    private volatile VirtualMachineMetrics virtualMachineMetrics;
    private volatile JsonObject previousMetrics = null;

    private final MetricsService metricsService = new MetricsService();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(METRICS_COLLECTOR_URL);
        properties.add(APPLICATION_ID);
        properties.add(HOSTNAME);
        properties.add(PROCESS_GROUP_ID);
        return properties;
    }

    @OnScheduled
    public void setup(final ConfigurationContext context) throws IOException {
        final Map<String, ?> config = Collections.emptyMap();
        factory = Json.createBuilderFactory(config);
        client = createClient();
        virtualMachineMetrics = VirtualMachineMetrics.getInstance();
        previousMetrics = null;
    }

    // used for testing to allow tests to override the client
    protected Client createClient() {
        return ClientBuilder.newClient();
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final String metricsCollectorUrl = context.getProperty(METRICS_COLLECTOR_URL).evaluateAttributeExpressions().getValue();
        final String applicationId = context.getProperty(APPLICATION_ID).evaluateAttributeExpressions().getValue();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();

        final boolean pgIdIsSet = context.getProperty(PROCESS_GROUP_ID).isSet();
        final String processGroupId = pgIdIsSet ? context.getProperty(PROCESS_GROUP_ID).evaluateAttributeExpressions().getValue() : null;

        final long start = System.currentTimeMillis();

        // send the metrics from last execution
        if (previousMetrics != null) {
            final WebTarget metricsTarget = client.target(metricsCollectorUrl);
            final Invocation.Builder invocation = metricsTarget.request();

            final Entity<String> entity = Entity.json(previousMetrics.toString());
            getLogger().debug("Sending metrics {} to Ambari", new Object[]{entity.getEntity()});

            final Response response = invocation.post(entity);
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                final long completedMillis = TimeUnit.NANOSECONDS.toMillis(System.currentTimeMillis() - start);
                getLogger().info("Successfully sent metrics to Ambari in {} ms", new Object[]{completedMillis});
            } else {
                final String responseEntity = response.hasEntity() ? response.readEntity(String.class) : "unknown error";
                getLogger().error("Error sending metrics to Ambari due to {} - {}", new Object[]{response.getStatus(), responseEntity});
            }
        }

        // calculate the current metrics, but store them to be sent next time
        final ProcessGroupStatus status = processGroupId == null ? context.getEventAccess().getControllerStatus() : context.getEventAccess().getGroupStatus(processGroupId);

        if(status != null) {
            final Map<String,String> statusMetrics = metricsService.getMetrics(status, pgIdIsSet);
            final Map<String,String> jvmMetrics = metricsService.getMetrics(virtualMachineMetrics);

            final MetricsBuilder metricsBuilder = new MetricsBuilder(factory);

            final JsonObject metricsObject = metricsBuilder
                    .applicationId(applicationId)
                    .instanceId(status.getId())
                    .hostname(hostname)
                    .timestamp(start)
                    .addAllMetrics(statusMetrics)
                    .addAllMetrics(jvmMetrics)
                    .build();

            previousMetrics = metricsObject;
        } else {
            getLogger().error("No process group status with ID = {}", new Object[]{processGroupId});
            previousMetrics = null;
        }
    }

}
