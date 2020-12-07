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

package org.apache.nifi.prometheus.util;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.SimpleCollector;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.TransmissionStatus;
import org.apache.nifi.controller.status.analytics.StatusAnalytics;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.metrics.jvm.JvmMetrics;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PrometheusMetricsUtil {

    public static final AllowableValue METRICS_STRATEGY_ROOT = new AllowableValue("Root Process Group", "Root Process Group",
            "Send rollup metrics for the entire root process group");
    public static final AllowableValue METRICS_STRATEGY_PG = new AllowableValue("All Process Groups", "All Process Groups",
            "Send metrics for each process group");
    public static final AllowableValue METRICS_STRATEGY_COMPONENTS = new AllowableValue("All Components", "All Components",
            "Send metrics for each component in the system, to include processors, connections, controller services, etc.");

    private static final CollectorRegistry CONNECTION_ANALYTICS_REGISTRY = new CollectorRegistry();
    private static final CollectorRegistry BULLETIN_REGISTRY = new CollectorRegistry();

    protected static final String DEFAULT_LABEL_STRING = "";

    // Common properties/values
    public static final AllowableValue CLIENT_NONE = new AllowableValue("No Authentication", "No Authentication",
            "ReportingTask will not authenticate clients. Anyone can communicate with this ReportingTask anonymously");
    public static final AllowableValue CLIENT_WANT = new AllowableValue("Want Authentication", "Want Authentication",
            "ReportingTask will try to verify the client but if unable to verify will allow the client to communicate anonymously");
    public static final AllowableValue CLIENT_NEED = new AllowableValue("Need Authentication", "Need Authentication",
            "ReportingTask will reject communications from any client unless the client provides a certificate that is trusted by the TrustStore"
                    + "specified in the SSL Context Service");

    public static final PropertyDescriptor METRICS_ENDPOINT_PORT = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-metrics-endpoint-port")
            .displayName("Prometheus Metrics Endpoint Port")
            .description("The Port where prometheus metrics can be accessed")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("9092")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor INSTANCE_ID = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-instance-id")
            .displayName("Instance ID")
            .description("Id of this NiFi instance to be included in the metrics sent to Prometheus")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("${hostname(true)}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_AUTH = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-client-auth")
            .displayName("Client Authentication")
            .description("Specifies whether or not the Reporting Task should authenticate clients. This value is ignored if the <SSL Context Service> "
                    + "Property is not specified or the SSL Context provided uses only a KeyStore and not a TrustStore.")
            .required(true)
            .allowableValues(CLIENT_NONE, CLIENT_WANT, CLIENT_NEED)
            .defaultValue(CLIENT_NONE.getValue())
            .build();

    public static CollectorRegistry createNifiMetrics(NiFiMetricsRegistry nifiMetricsRegistry, ProcessGroupStatus status,
                                                      String instId, String parentProcessGroupId, String compType, String metricsStrategy) {

        final String instanceId = StringUtils.isEmpty(instId) ? DEFAULT_LABEL_STRING : instId;
        final String parentPGId = StringUtils.isEmpty(parentProcessGroupId) ? DEFAULT_LABEL_STRING : parentProcessGroupId;
        final String componentType = StringUtils.isEmpty(compType) ? DEFAULT_LABEL_STRING : compType;
        final String componentId = StringUtils.isEmpty(status.getId()) ? DEFAULT_LABEL_STRING : status.getId();
        final String componentName = StringUtils.isEmpty(status.getName()) ? DEFAULT_LABEL_STRING : status.getName();

        // Clear all collectors to deal with removed/renamed components -- for root PG only
        if("RootProcessGroup".equals(componentType)) {
            try {
                for (final Field field : PrometheusMetricsUtil.class.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers()) && (field.get(null) instanceof SimpleCollector)) {
                        SimpleCollector<?> sc = (SimpleCollector<?>) (field.get(null));
                        sc.clear();
                    }
                }
            } catch (IllegalAccessException e) {
                // ignore
            }
        }

        nifiMetricsRegistry.setDataPoint(status.getFlowFilesSent(), "AMOUNT_FLOWFILES_SENT", instanceId, componentType, componentName, componentId, parentPGId);
        nifiMetricsRegistry.setDataPoint(status.getFlowFilesTransferred(), "AMOUNT_FLOWFILES_TRANSFERRED", instanceId, componentType, componentName, componentId, parentPGId);
        nifiMetricsRegistry.setDataPoint(status.getFlowFilesReceived(), "AMOUNT_FLOWFILES_RECEIVED", instanceId, componentType, componentName, componentId, parentPGId);

        nifiMetricsRegistry.setDataPoint(status.getBytesSent(), "AMOUNT_BYTES_SENT", instanceId, componentType, componentName, componentId, parentPGId);
        nifiMetricsRegistry.setDataPoint(status.getBytesRead(), "AMOUNT_BYTES_READ", instanceId, componentType, componentName, componentId, parentPGId);
        nifiMetricsRegistry.setDataPoint(status.getBytesWritten(), "AMOUNT_BYTES_WRITTEN", instanceId, componentType, componentName, componentId, parentPGId);
        nifiMetricsRegistry.setDataPoint(status.getBytesReceived(), "AMOUNT_BYTES_RECEIVED", instanceId, componentType, componentName, componentId, parentPGId);
        nifiMetricsRegistry.setDataPoint(status.getBytesTransferred(), "AMOUNT_BYTES_TRANSFERRED", instanceId, componentType, componentName, componentId, parentPGId);

        nifiMetricsRegistry.setDataPoint(status.getOutputContentSize(), "SIZE_CONTENT_OUTPUT_TOTAL",
                instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "");
        nifiMetricsRegistry.setDataPoint(status.getInputContentSize(), "SIZE_CONTENT_INPUT_TOTAL",
                instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "");
        nifiMetricsRegistry.setDataPoint(status.getQueuedContentSize(), "SIZE_CONTENT_QUEUED_TOTAL",
                instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "");

        nifiMetricsRegistry.setDataPoint(status.getOutputCount(), "AMOUNT_ITEMS_OUTPUT", instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "");
        nifiMetricsRegistry.setDataPoint(status.getInputCount(), "AMOUNT_ITEMS_INPUT", instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "");
        nifiMetricsRegistry.setDataPoint(status.getQueuedCount(), "AMOUNT_ITEMS_QUEUED", instanceId, componentType, componentName, componentId, parentPGId,"", "", "", "");

        nifiMetricsRegistry.setDataPoint(status.getActiveThreadCount() == null ? 0 : status.getActiveThreadCount(), "AMOUNT_THREADS_TOTAL_ACTIVE",
                instanceId, componentType, componentName, componentId, parentPGId);
        nifiMetricsRegistry.setDataPoint(status.getTerminatedThreadCount() == null ? 0 : status.getTerminatedThreadCount(), "AMOUNT_THREADS_TOTAL_TERMINATED",
                instanceId, componentType, componentName, componentId, parentPGId);

        // Report metrics for child process groups if specified
        if (METRICS_STRATEGY_PG.getValue().equals(metricsStrategy) || METRICS_STRATEGY_COMPONENTS.getValue().equals(metricsStrategy)) {
            status.getProcessGroupStatus().forEach((childGroupStatus) -> createNifiMetrics(nifiMetricsRegistry, childGroupStatus, instanceId, componentId, "ProcessGroup", metricsStrategy));
        }

        if (METRICS_STRATEGY_COMPONENTS.getValue().equals(metricsStrategy)) {
            // Report metrics for all components
            for (ProcessorStatus processorStatus : status.getProcessorStatus()) {
                Map<String, Long> counters = processorStatus.getCounters();

                if (counters != null) {
                    counters.entrySet().stream().forEach(entry -> nifiMetricsRegistry.setDataPoint(entry.getValue(), "PROCESSOR_COUNTERS",
                            processorStatus.getName(), entry.getKey(), processorStatus.getId(), instanceId));
                }

                final String procComponentType = "Processor";
                final String procComponentId = StringUtils.isEmpty(processorStatus.getId()) ? DEFAULT_LABEL_STRING : processorStatus.getId();
                final String procComponentName = StringUtils.isEmpty(processorStatus.getName()) ? DEFAULT_LABEL_STRING : processorStatus.getName();
                final String parentId = StringUtils.isEmpty(processorStatus.getGroupId()) ? DEFAULT_LABEL_STRING : processorStatus.getGroupId();

                nifiMetricsRegistry.setDataPoint(processorStatus.getFlowFilesSent(), "AMOUNT_FLOWFILES_SENT", instanceId, procComponentType, procComponentName, procComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(processorStatus.getFlowFilesReceived(), "AMOUNT_FLOWFILES_RECEIVED",
                        instanceId, procComponentType, procComponentName, procComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(processorStatus.getFlowFilesRemoved(), "AMOUNT_FLOWFILES_REMOVED",
                        instanceId, procComponentType, procComponentName, procComponentId, parentId);

                nifiMetricsRegistry.setDataPoint(processorStatus.getBytesSent(), "AMOUNT_BYTES_SENT", instanceId, procComponentType, procComponentName, procComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(processorStatus.getBytesRead(), "AMOUNT_BYTES_READ", instanceId, procComponentType, procComponentName, procComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(processorStatus.getBytesWritten(), "AMOUNT_BYTES_WRITTEN", instanceId, procComponentType, procComponentName, procComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(processorStatus.getBytesReceived(), "AMOUNT_BYTES_RECEIVED", instanceId, procComponentType, procComponentName, procComponentId, parentId);

                nifiMetricsRegistry.setDataPoint(processorStatus.getOutputBytes(), "SIZE_CONTENT_OUTPUT_TOTAL",
                        instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "");
                nifiMetricsRegistry.setDataPoint(processorStatus.getInputBytes(), "SIZE_CONTENT_INPUT_TOTAL",
                        instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "");

                nifiMetricsRegistry.setDataPoint(processorStatus.getOutputCount(), "AMOUNT_ITEMS_OUTPUT",
                        instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "");
                nifiMetricsRegistry.setDataPoint(processorStatus.getInputCount(), "AMOUNT_ITEMS_INPUT",
                        instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "");

                nifiMetricsRegistry.setDataPoint(processorStatus.getAverageLineageDuration(), "AVERAGE_LINEAGE_DURATION",
                        instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "");

                nifiMetricsRegistry.setDataPoint(status.getActiveThreadCount() == null ? 0 : status.getActiveThreadCount(), "AMOUNT_THREADS_TOTAL_ACTIVE",
                        instanceId, procComponentType, procComponentName, procComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(status.getTerminatedThreadCount() == null ? 0 : status.getTerminatedThreadCount(), "AMOUNT_THREADS_TOTAL_TERMINATED",
                        instanceId, procComponentType, procComponentName, procComponentId, parentId);

            }
            for (ConnectionStatus connectionStatus : status.getConnectionStatus()) {
                final String connComponentId = StringUtils.isEmpty(connectionStatus.getId()) ? DEFAULT_LABEL_STRING : connectionStatus.getId();
                final String connComponentName = StringUtils.isEmpty(connectionStatus.getName()) ? DEFAULT_LABEL_STRING : connectionStatus.getName();
                final String sourceId = StringUtils.isEmpty(connectionStatus.getSourceId()) ? DEFAULT_LABEL_STRING : connectionStatus.getSourceId();
                final String sourceName = StringUtils.isEmpty(connectionStatus.getSourceName()) ? DEFAULT_LABEL_STRING : connectionStatus.getSourceName();
                final String destinationId = StringUtils.isEmpty(connectionStatus.getDestinationId()) ? DEFAULT_LABEL_STRING : connectionStatus.getDestinationId();
                final String destinationName = StringUtils.isEmpty(connectionStatus.getDestinationName()) ? DEFAULT_LABEL_STRING : connectionStatus.getDestinationName();
                final String parentId = StringUtils.isEmpty(connectionStatus.getGroupId()) ? DEFAULT_LABEL_STRING : connectionStatus.getGroupId();
                final String connComponentType = "Connection";
                nifiMetricsRegistry.setDataPoint(connectionStatus.getOutputBytes(), "SIZE_CONTENT_OUTPUT_TOTAL",
                        instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);
                nifiMetricsRegistry.setDataPoint(connectionStatus.getInputBytes(), "SIZE_CONTENT_INPUT_TOTAL",
                        instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);
                nifiMetricsRegistry.setDataPoint(connectionStatus.getQueuedBytes(), "SIZE_CONTENT_QUEUED_TOTAL",
                        instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);

                nifiMetricsRegistry.setDataPoint(connectionStatus.getOutputCount(), "AMOUNT_ITEMS_OUTPUT",
                        instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);
                nifiMetricsRegistry.setDataPoint(connectionStatus.getInputCount(), "AMOUNT_ITEMS_INPUT",
                        instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);
                nifiMetricsRegistry.setDataPoint(connectionStatus.getQueuedCount(), "AMOUNT_ITEMS_QUEUED",
                        instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);

                nifiMetricsRegistry.setDataPoint(connectionStatus.getBackPressureBytesThreshold(), "BACKPRESSURE_BYTES_THRESHOLD",
                        instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);
                nifiMetricsRegistry.setDataPoint(connectionStatus.getBackPressureObjectThreshold(), "BACKPRESSURE_OBJECT_THRESHOLD",
                        instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);

                nifiMetricsRegistry.setDataPoint(getUtilization(connectionStatus.getQueuedBytes(), connectionStatus.getBackPressureBytesThreshold()),
                        "PERCENT_USED_BYTES", instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);
                nifiMetricsRegistry.setDataPoint(getUtilization(connectionStatus.getQueuedCount(), connectionStatus.getBackPressureObjectThreshold()),
                        "PERCENT_USED_COUNT", instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);

                boolean isBackpressureEnabled = (connectionStatus.getBackPressureObjectThreshold() > 0 && connectionStatus.getBackPressureObjectThreshold() <= connectionStatus.getQueuedCount())
                        || (connectionStatus.getBackPressureBytesThreshold() > 0 && connectionStatus.getBackPressureBytesThreshold() <= connectionStatus.getQueuedBytes());
                nifiMetricsRegistry.setDataPoint(isBackpressureEnabled ? 1 : 0, "IS_BACKPRESSURE_ENABLED",
                        instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);
            }
            for (PortStatus portStatus : status.getInputPortStatus()) {
                final String portComponentId = StringUtils.isEmpty(portStatus.getId()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String portComponentName = StringUtils.isEmpty(portStatus.getName()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String parentId = StringUtils.isEmpty(portStatus.getGroupId()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String portComponentType = "InputPort";
                nifiMetricsRegistry.setDataPoint(portStatus.getFlowFilesSent(), "AMOUNT_FLOWFILES_SENT", instanceId, portComponentType, portComponentName, portComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(portStatus.getFlowFilesReceived(), "AMOUNT_FLOWFILES_RECEIVED", instanceId, portComponentType, portComponentName, portComponentId, parentId);

                nifiMetricsRegistry.setDataPoint(portStatus.getBytesSent(), "AMOUNT_BYTES_SENT", instanceId, portComponentType, portComponentName, portComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(portStatus.getInputBytes(), "AMOUNT_BYTES_READ", instanceId, portComponentType, portComponentName, portComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(portStatus.getOutputBytes(), "AMOUNT_BYTES_WRITTEN", instanceId, portComponentType, portComponentName, portComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(portStatus.getBytesReceived(), "AMOUNT_BYTES_RECEIVED", instanceId, portComponentType, portComponentName, portComponentId, parentId);

                nifiMetricsRegistry.setDataPoint(portStatus.getOutputCount(), "AMOUNT_ITEMS_OUTPUT",
                        instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "");
                nifiMetricsRegistry.setDataPoint(portStatus.getInputCount(), "AMOUNT_ITEMS_INPUT",
                        instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "");

                final Boolean isTransmitting = portStatus.isTransmitting();
                nifiMetricsRegistry.setDataPoint(isTransmitting == null ? 0 : (isTransmitting ? 1 : 0), "IS_TRANSMITTING",
                        instanceId, portComponentType, portComponentName, portComponentId, parentId, portStatus.getRunStatus().name());

                nifiMetricsRegistry.setDataPoint(portStatus.getActiveThreadCount(), "AMOUNT_THREADS_TOTAL_ACTIVE", instanceId, portComponentType, portComponentName, portComponentId, parentId);
            }
            for (PortStatus portStatus : status.getOutputPortStatus()) {
                final String portComponentId = StringUtils.isEmpty(portStatus.getId()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String portComponentName = StringUtils.isEmpty(portStatus.getName()) ? DEFAULT_LABEL_STRING : portStatus.getName();
                final String parentId = StringUtils.isEmpty(portStatus.getGroupId()) ? DEFAULT_LABEL_STRING : portStatus.getGroupId();
                final String portComponentType = "OutputPort";
                nifiMetricsRegistry.setDataPoint(portStatus.getFlowFilesSent(), "AMOUNT_FLOWFILES_SENT", instanceId, portComponentType, portComponentName, portComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(portStatus.getFlowFilesReceived(), "AMOUNT_FLOWFILES_RECEIVED", instanceId, portComponentType, portComponentName, portComponentId, parentId);

                nifiMetricsRegistry.setDataPoint(portStatus.getBytesSent(), "AMOUNT_BYTES_SENT", instanceId, portComponentType, portComponentName, portComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(portStatus.getInputBytes(), "AMOUNT_BYTES_READ", instanceId, portComponentType, portComponentName, portComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(portStatus.getOutputBytes(), "AMOUNT_BYTES_WRITTEN", instanceId, portComponentType, portComponentName, portComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(portStatus.getBytesReceived(), "AMOUNT_BYTES_RECEIVED", instanceId, portComponentType, portComponentName, portComponentId, parentId);

                nifiMetricsRegistry.setDataPoint(portStatus.getOutputCount(), "AMOUNT_ITEMS_OUTPUT",
                        instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "");
                nifiMetricsRegistry.setDataPoint(portStatus.getInputCount(), "AMOUNT_ITEMS_INPUT",
                        instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "");

                final Boolean isTransmitting = portStatus.isTransmitting();
                nifiMetricsRegistry.setDataPoint(isTransmitting == null ? 0 : (isTransmitting ? 1 : 0), "IS_TRANSMITTING",
                        instanceId, portComponentType, portComponentName, portComponentId, parentId, portStatus.getRunStatus().name());

                nifiMetricsRegistry.setDataPoint(portStatus.getActiveThreadCount(), "AMOUNT_THREADS_TOTAL_ACTIVE", instanceId, portComponentType, portComponentName, portComponentId, parentId);
            }
            for (RemoteProcessGroupStatus remoteProcessGroupStatus : status.getRemoteProcessGroupStatus()) {
                final String rpgComponentId = StringUtils.isEmpty(remoteProcessGroupStatus.getId()) ? DEFAULT_LABEL_STRING : remoteProcessGroupStatus.getId();
                final String rpgComponentName = StringUtils.isEmpty(remoteProcessGroupStatus.getName()) ? DEFAULT_LABEL_STRING : remoteProcessGroupStatus.getName();
                final String parentId = StringUtils.isEmpty(remoteProcessGroupStatus.getGroupId()) ? DEFAULT_LABEL_STRING : remoteProcessGroupStatus.getGroupId();
                final String rpgComponentType = "RemoteProcessGroup";

                nifiMetricsRegistry.setDataPoint(remoteProcessGroupStatus.getSentContentSize(), "AMOUNT_BYTES_WRITTEN", instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId);
                nifiMetricsRegistry.setDataPoint(remoteProcessGroupStatus.getReceivedContentSize(), "AMOUNT_BYTES_RECEIVED", instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId);

                nifiMetricsRegistry.setDataPoint(remoteProcessGroupStatus.getSentCount(), "AMOUNT_ITEMS_OUTPUT",
                        instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "");
                nifiMetricsRegistry.setDataPoint(remoteProcessGroupStatus.getReceivedCount(), "AMOUNT_ITEMS_INPUT",
                        instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "");

                nifiMetricsRegistry.setDataPoint(remoteProcessGroupStatus.getActiveRemotePortCount(), "ACTIVE_REMOTE_PORT_COUNT",
                        instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "");
                nifiMetricsRegistry.setDataPoint(remoteProcessGroupStatus.getInactiveRemotePortCount(), "INACTIVE_REMOTE_PORT_COUNT",
                        instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "");

                nifiMetricsRegistry.setDataPoint(remoteProcessGroupStatus.getAverageLineageDuration(), "AVERAGE_LINEAGE_DURATION",
                        instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "");

                nifiMetricsRegistry.setDataPoint(TransmissionStatus.Transmitting.equals(remoteProcessGroupStatus.getTransmissionStatus()) ? 1 : 0, "IS_TRANSMITTING",
                        instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, remoteProcessGroupStatus.getTransmissionStatus().name());

                nifiMetricsRegistry.setDataPoint(remoteProcessGroupStatus.getActiveThreadCount(), "AMOUNT_THREADS_TOTAL_ACTIVE",
                        instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId);
            }
        }

        return nifiMetricsRegistry.getRegistry();
    }

    public static CollectorRegistry createJvmMetrics(JvmMetricsRegistry jvmMetricsRegistry, JvmMetrics jvmMetrics, String instId) {
        final String instanceId = StringUtils.isEmpty(instId) ? DEFAULT_LABEL_STRING : instId;
        jvmMetricsRegistry.setDataPoint(jvmMetrics.heapUsed(DataUnit.B), "JVM_HEAP_USED", instanceId);
        jvmMetricsRegistry.setDataPoint(jvmMetrics.heapUsage(), "JVM_HEAP_USAGE", instanceId);
        jvmMetricsRegistry.setDataPoint(jvmMetrics.nonHeapUsage(), "JVM_HEAP_NON_USAGE", instanceId);
        jvmMetricsRegistry.setDataPoint(jvmMetrics.threadCount(), "JVM_THREAD_COUNT", instanceId);
        jvmMetricsRegistry.setDataPoint(jvmMetrics.daemonThreadCount(), "JVM_DAEMON_THREAD_COUNT", instanceId);
        jvmMetricsRegistry.setDataPoint(jvmMetrics.uptime(), "JVM_UPTIME", instanceId);
        jvmMetricsRegistry.setDataPoint(jvmMetrics.fileDescriptorUsage(), "JVM_FILE_DESCRIPTOR_USAGE", instanceId);

        jvmMetrics.garbageCollectors()
                .forEach((name, stat) -> {
                    jvmMetricsRegistry.setDataPoint(stat.getRuns(), "JVM_GC_RUNS", instanceId, name);
                    jvmMetricsRegistry.setDataPoint(stat.getTime(TimeUnit.MILLISECONDS), "JVM_GC_TIME", instanceId, name);
                });

        return jvmMetricsRegistry.getRegistry();
    }

    public static CollectorRegistry createConnectionStatusAnalyticsMetrics(ConnectionAnalyticsMetricsRegistry connectionAnalyticsMetricsRegistry, StatusAnalytics statusAnalytics,
                                                                           String instId, String connComponentType, String connName,
                                                                           String connId, String pgId, String srcId, String srcName, String destId, String destName) {
        if(statusAnalytics != null) {
            final String instanceId = StringUtils.isEmpty(instId) ? DEFAULT_LABEL_STRING : instId;
            final String connComponentId = StringUtils.isEmpty(connId) ? DEFAULT_LABEL_STRING : connId;
            final String connComponentName = StringUtils.isEmpty(connName) ? DEFAULT_LABEL_STRING : connName;
            final String sourceId = StringUtils.isEmpty(srcId) ? DEFAULT_LABEL_STRING : srcId;
            final String sourceName = StringUtils.isEmpty(srcName) ? DEFAULT_LABEL_STRING : srcName;
            final String destinationId = StringUtils.isEmpty(destId) ? DEFAULT_LABEL_STRING : destId;
            final String destinationName = StringUtils.isEmpty(destName) ? DEFAULT_LABEL_STRING : destName;
            final String parentId = StringUtils.isEmpty(pgId) ? DEFAULT_LABEL_STRING : pgId;


            Map<String, Long> predictions = statusAnalytics.getPredictions();
            connectionAnalyticsMetricsRegistry.setDataPoint(predictions.get("timeToBytesBackpressureMillis"),
                    "TIME_TO_BYTES_BACKPRESSURE_PREDICTION",
                    instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);
            connectionAnalyticsMetricsRegistry.setDataPoint(predictions.get("timeToCountBackpressureMillis"),
                    "TIME_TO_COUNT_BACKPRESSURE_PREDICTION",
                    instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);
            connectionAnalyticsMetricsRegistry.setDataPoint(predictions.get("nextIntervalBytes"),
                    "BYTES_AT_NEXT_INTERVAL_PREDICTION",
                    instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);
            connectionAnalyticsMetricsRegistry.setDataPoint(predictions.get("nextIntervalCount"),
                    "COUNT_AT_NEXT_INTERVAL_PREDICTION",
                    instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName);
        }

        return connectionAnalyticsMetricsRegistry.getRegistry();
    }

    private static double getUtilization(final double used, final double total) {
        return (used / total) * 100;
    }

    public static CollectorRegistry createBulletinMetrics(BulletinMetricsRegistry bulletinMetricsRegistry, String instId, String compType, String compId, String pgId, String nodeAddr,
                                                          String cat, String srcName, String srcId, String lvl) {
        final String instanceId = StringUtils.isEmpty(instId) ? DEFAULT_LABEL_STRING : instId;
        final String componentType = StringUtils.isEmpty(compType) ? DEFAULT_LABEL_STRING : compType;
        final String componentId = StringUtils.isEmpty(compId) ? DEFAULT_LABEL_STRING : compId;
        final String sourceId = StringUtils.isEmpty(srcId) ? DEFAULT_LABEL_STRING : srcId;
        final String sourceName = StringUtils.isEmpty(srcName) ? DEFAULT_LABEL_STRING : srcName;
        final String nodeAddress = StringUtils.isEmpty(nodeAddr) ? DEFAULT_LABEL_STRING : nodeAddr;
        final String category = StringUtils.isEmpty(cat) ? DEFAULT_LABEL_STRING : cat;
        final String parentId = StringUtils.isEmpty(pgId) ? DEFAULT_LABEL_STRING : pgId;
        final String level = StringUtils.isEmpty(lvl) ? DEFAULT_LABEL_STRING : lvl;
        bulletinMetricsRegistry.setDataPoint(1, "BULLETIN", instanceId, componentType, componentId, parentId, nodeAddress, category, sourceName, sourceId, level);
        return bulletinMetricsRegistry.getRegistry();
    }
}
