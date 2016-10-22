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
package org.apache.nifi.reporting.datadog;


import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AtomicDouble;
import com.yammer.metrics.core.VirtualMachineMetrics;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.datadog.metrics.MetricsService;
import org.coursera.metrics.datadog.DynamicTagsCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;



@Tags({"reporting", "datadog", "metrics"})
@CapabilityDescription("Publishes metrics from NiFi to datadog. For accurate and informative reporting, components should have unique names.")
public class DataDogReportingTask extends AbstractReportingTask {

    static final AllowableValue DATADOG_AGENT = new AllowableValue("Datadog Agent", "Datadog Agent",
            "Metrics will be sent via locally installed Datadog agent. " +
                    "Datadog agent needs to be installed manually before using this option");

    static final AllowableValue DATADOG_HTTP = new AllowableValue("Datadog HTTP", "Datadog HTTP",
            "Metrics will be sent via HTTP transport with no need of Agent installed. " +
                    "Datadog API key needs to be set");

    static final PropertyDescriptor DATADOG_TRANSPORT = new PropertyDescriptor.Builder()
            .name("Datadog transport")
            .description("Transport through which metrics will be sent to Datadog")
            .required(true)
            .allowableValues(DATADOG_AGENT, DATADOG_HTTP)
            .defaultValue(DATADOG_HTTP.getValue())
            .build();

    static final PropertyDescriptor API_KEY = new PropertyDescriptor.Builder()
            .name("API key")
            .description("Datadog API key. If specified value is 'agent', local Datadog agent will be used.")
            .expressionLanguageSupported(false)
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor METRICS_PREFIX = new PropertyDescriptor.Builder()
            .name("Metrics prefix")
            .description("Prefix to be added before every metric")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor ENVIRONMENT = new PropertyDescriptor.Builder()
            .name("Environment")
            .description("Environment, dataflow is running in. " +
                    "This property will be included as metrics tag.")
            .required(true)
            .expressionLanguageSupported(true)
            .defaultValue("dev")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private MetricsService metricsService;
    private DDMetricRegistryBuilder ddMetricRegistryBuilder;
    private MetricRegistry metricRegistry;
    private String metricsPrefix;
    private String environment;
    private String statusId;
    private ConcurrentHashMap<String, AtomicDouble> metricsMap;
    private Map<String, String> defaultTags;
    private volatile VirtualMachineMetrics virtualMachineMetrics;
    private Logger logger = LoggerFactory.getLogger(getClass().getName());

    @OnScheduled
    public void setup(final ConfigurationContext context) {
        metricsService = getMetricsService();
        ddMetricRegistryBuilder = getMetricRegistryBuilder();
        metricRegistry = getMetricRegistry();
        metricsMap = getMetricsMap();
        metricsPrefix = METRICS_PREFIX.getDefaultValue();
        environment = ENVIRONMENT.getDefaultValue();
        virtualMachineMetrics = VirtualMachineMetrics.getInstance();
        ddMetricRegistryBuilder.setMetricRegistry(metricRegistry)
                .setTags(metricsService.getAllTagsList());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(METRICS_PREFIX);
        properties.add(ENVIRONMENT);
        properties.add(API_KEY);
        properties.add(DATADOG_TRANSPORT);
        return properties;
    }

    @Override
    public void onTrigger(ReportingContext context) {
        final ProcessGroupStatus status = context.getEventAccess().getControllerStatus();

        metricsPrefix = context.getProperty(METRICS_PREFIX).evaluateAttributeExpressions().getValue();
        environment = context.getProperty(ENVIRONMENT).evaluateAttributeExpressions().getValue();
        statusId = status.getId();
        defaultTags = ImmutableMap.of("env", environment, "dataflow_id", statusId);
        try {
            updateDataDogTransport(context);
        } catch (IOException e) {
            e.printStackTrace();
        }
        updateAllMetricGroups(status);
        ddMetricRegistryBuilder.getDatadogReporter().report();
    }

    protected void updateMetrics(Map<String, Double> metrics, Optional<String> processorName, Map<String, String> tags) {
        for (Map.Entry<String, Double> entry : metrics.entrySet()) {
            final String metricName = buildMetricName(processorName, entry.getKey());
            logger.debug(metricName + ": " + entry.getValue());
            //if metric is not registered yet - register it
            if (!metricsMap.containsKey(metricName)) {
                metricsMap.put(metricName, new AtomicDouble(entry.getValue()));
                metricRegistry.register(metricName, new MetricGauge(metricName, tags));
            }
            //set real time value to metrics map
            metricsMap.get(metricName).set(entry.getValue());
        }
    }

    private void updateAllMetricGroups(ProcessGroupStatus processGroupStatus) {
        final List<ProcessorStatus> processorStatuses = new ArrayList<>();
        populateProcessorStatuses(processGroupStatus, processorStatuses);
        for (final ProcessorStatus processorStatus : processorStatuses) {
            updateMetrics(metricsService.getProcessorMetrics(processorStatus),
                    Optional.of(processorStatus.getName()), defaultTags);
        }

        final List<ConnectionStatus> connectionStatuses = new ArrayList<>();
        populateConnectionStatuses(processGroupStatus, connectionStatuses);
        for (ConnectionStatus connectionStatus: connectionStatuses) {
            Map<String, String> connectionStatusTags = new HashMap<>(defaultTags);
            connectionStatusTags.putAll(metricsService.getConnectionStatusTags(connectionStatus));
            updateMetrics(metricsService.getConnectionStatusMetrics(connectionStatus), Optional.<String>absent(), connectionStatusTags);
        }

        final List<PortStatus> inputPortStatuses = new ArrayList<>();
        populateInputPortStatuses(processGroupStatus, inputPortStatuses);
        for (PortStatus portStatus: inputPortStatuses) {
            Map<String, String> portTags = new HashMap<>(defaultTags);
            portTags.putAll(metricsService.getPortStatusTags(portStatus));
            updateMetrics(metricsService.getPortStatusMetrics(portStatus), Optional.<String>absent(), portTags);
        }

        final List<PortStatus> outputPortStatuses = new ArrayList<>();
        populateOutputPortStatuses(processGroupStatus, outputPortStatuses);
        for (PortStatus portStatus: outputPortStatuses) {
            Map<String, String> portTags = new HashMap<>(defaultTags);
            portTags.putAll(metricsService.getPortStatusTags(portStatus));
            updateMetrics(metricsService.getPortStatusMetrics(portStatus), Optional.<String>absent(), portTags);
        }

        updateMetrics(metricsService.getJVMMetrics(virtualMachineMetrics),
                Optional.<String>absent(), defaultTags);
        updateMetrics(metricsService.getDataFlowMetrics(processGroupStatus), Optional.<String>absent(), defaultTags);
    }

    private class MetricGauge implements Gauge, DynamicTagsCallback {
        private Map<String, String> tags;
        private String metricName;

        public MetricGauge(String metricName, Map<String, String> tagsMap) {
            this.tags = tagsMap;
            this.metricName = metricName;
        }

        @Override
        public Object getValue() {
            return metricsMap.get(metricName).get();
        }

        @Override
        public List<String> getTags() {
            List<String> tagsList = Lists.newArrayList();
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                tagsList.add(entry.getKey() + ":" + entry.getValue());
            }
            return tagsList;
        }
    }

    private void updateDataDogTransport(ReportingContext context) throws IOException {
        String dataDogTransport = context.getProperty(DATADOG_TRANSPORT).getValue();
        if (dataDogTransport.equalsIgnoreCase(DATADOG_AGENT.getValue())) {
            ddMetricRegistryBuilder.build("agent");
        } else if (dataDogTransport.equalsIgnoreCase(DATADOG_HTTP.getValue())
                && context.getProperty(API_KEY).isSet()) {
            ddMetricRegistryBuilder.build(context.getProperty(API_KEY).getValue());
        }
    }

    private void populateProcessorStatuses(final ProcessGroupStatus groupStatus, final List<ProcessorStatus> statuses) {
        statuses.addAll(groupStatus.getProcessorStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateProcessorStatuses(childGroupStatus, statuses);
        }
    }

    private void populateConnectionStatuses(final ProcessGroupStatus groupStatus, final List<ConnectionStatus> statuses) {
        statuses.addAll(groupStatus.getConnectionStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateConnectionStatuses(childGroupStatus, statuses);
        }
    }

    private void populateInputPortStatuses(final ProcessGroupStatus groupStatus, final List<PortStatus> statuses) {
        statuses.addAll(groupStatus.getInputPortStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateInputPortStatuses(childGroupStatus, statuses);
        }
    }

    private void populateOutputPortStatuses(final ProcessGroupStatus groupStatus, final List<PortStatus> statuses) {
        statuses.addAll(groupStatus.getOutputPortStatus());
        for (final ProcessGroupStatus childGroupStatus : groupStatus.getProcessGroupStatus()) {
            populateOutputPortStatuses(childGroupStatus, statuses);
        }
    }

    private String buildMetricName(Optional<String> processorName, String metricName) {
        return metricsPrefix + "." + processorName.or("flow") + "." + metricName;
    }

    protected MetricsService getMetricsService() {
        return new MetricsService();
    }

    protected DDMetricRegistryBuilder getMetricRegistryBuilder() {
        return new DDMetricRegistryBuilder();
    }

    protected MetricRegistry getMetricRegistry() {
        return new MetricRegistry();
    }

    protected ConcurrentHashMap<String, AtomicDouble> getMetricsMap() {
        return new ConcurrentHashMap<>();
    }
}