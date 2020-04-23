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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.prometheus.client.SimpleCollector;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;

import io.prometheus.client.CollectorRegistry;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.TransmissionStatus;
import org.apache.nifi.controller.status.analytics.StatusAnalytics;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.metrics.jvm.JvmMetrics;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

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

    public static CollectorRegistry createNifiMetrics(NiFiMetricsHolder nifiMetricsHolder, ProcessGroupStatus status,
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

        nifiMetricsHolder.AMOUNT_FLOWFILES_SENT.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getFlowFilesSent());
        nifiMetricsHolder.AMOUNT_FLOWFILES_TRANSFERRED.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getFlowFilesTransferred());
        nifiMetricsHolder.AMOUNT_FLOWFILES_RECEIVED.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getFlowFilesReceived());

        nifiMetricsHolder.AMOUNT_BYTES_SENT.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getBytesSent());
        nifiMetricsHolder.AMOUNT_BYTES_READ.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getBytesRead());
        nifiMetricsHolder.AMOUNT_BYTES_WRITTEN.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getBytesWritten());
        nifiMetricsHolder.TOTAL_BYTES_READ.labels(instanceId, componentType, componentName, componentId, parentPGId).inc(status.getBytesRead());
        nifiMetricsHolder.TOTAL_BYTES_WRITTEN.labels(instanceId, componentType, componentName, componentId, parentPGId).inc(status.getBytesWritten());
        nifiMetricsHolder.AMOUNT_BYTES_RECEIVED.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getBytesReceived());
        nifiMetricsHolder.AMOUNT_BYTES_TRANSFERRED.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getBytesTransferred());

        nifiMetricsHolder.SIZE_CONTENT_OUTPUT_TOTAL.labels(instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "")
                .set(status.getOutputContentSize());
        nifiMetricsHolder.SIZE_CONTENT_INPUT_TOTAL.labels(instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "")
                .set(status.getInputContentSize());
        nifiMetricsHolder.SIZE_CONTENT_QUEUED_TOTAL.labels(instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "")
                .set(status.getQueuedContentSize());

        nifiMetricsHolder.AMOUNT_ITEMS_OUTPUT.labels(instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "")
                .set(status.getOutputCount());
        nifiMetricsHolder.AMOUNT_ITEMS_INPUT.labels(instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "")
                .set(status.getInputCount());
        nifiMetricsHolder.AMOUNT_ITEMS_QUEUED.labels(instanceId, componentType, componentName, componentId, parentPGId,"", "", "", "")
                .set(status.getQueuedCount());

        nifiMetricsHolder.AMOUNT_THREADS_TOTAL_ACTIVE.labels(instanceId, componentType, componentName, componentId, parentPGId)
                .set(status.getActiveThreadCount() == null ? 0 : status.getActiveThreadCount());
        nifiMetricsHolder.AMOUNT_THREADS_TOTAL_TERMINATED.labels(instanceId, componentType, componentName, componentId, parentPGId)
                .set(status.getTerminatedThreadCount() == null ? 0 : status.getTerminatedThreadCount());

        // Report metrics for child process groups if specified
        if (METRICS_STRATEGY_PG.getValue().equals(metricsStrategy) || METRICS_STRATEGY_COMPONENTS.getValue().equals(metricsStrategy)) {
            status.getProcessGroupStatus().forEach((childGroupStatus) -> createNifiMetrics(nifiMetricsHolder, childGroupStatus, instanceId, componentId, "ProcessGroup", metricsStrategy));
        }

        if (METRICS_STRATEGY_COMPONENTS.getValue().equals(metricsStrategy)) {
            // Report metrics for all components
            for (ProcessorStatus processorStatus : status.getProcessorStatus()) {
                Map<String, Long> counters = processorStatus.getCounters();

                if (counters != null) {
                    counters.entrySet().stream().forEach(entry -> nifiMetricsHolder.PROCESSOR_COUNTERS
                            .labels(processorStatus.getName(), entry.getKey(), processorStatus.getId(), instanceId).set(entry.getValue()));
                }

                final String procComponentType = "Processor";
                final String procComponentId = StringUtils.isEmpty(processorStatus.getId()) ? DEFAULT_LABEL_STRING : processorStatus.getId();
                final String procComponentName = StringUtils.isEmpty(processorStatus.getName()) ? DEFAULT_LABEL_STRING : processorStatus.getName();
                final String parentId = StringUtils.isEmpty(processorStatus.getGroupId()) ? DEFAULT_LABEL_STRING : processorStatus.getGroupId();

                nifiMetricsHolder.AMOUNT_FLOWFILES_SENT.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getFlowFilesSent());
                nifiMetricsHolder.AMOUNT_FLOWFILES_RECEIVED.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getFlowFilesReceived());
                nifiMetricsHolder.AMOUNT_FLOWFILES_REMOVED.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getFlowFilesRemoved());

                nifiMetricsHolder.AMOUNT_BYTES_SENT.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getBytesSent());
                nifiMetricsHolder.AMOUNT_BYTES_READ.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getBytesRead());
                nifiMetricsHolder.AMOUNT_BYTES_WRITTEN.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getBytesWritten());
                nifiMetricsHolder.TOTAL_BYTES_READ.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).inc(status.getBytesRead());
                nifiMetricsHolder.TOTAL_BYTES_WRITTEN.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).inc(status.getBytesWritten());
                nifiMetricsHolder.AMOUNT_BYTES_RECEIVED.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getBytesReceived());

                nifiMetricsHolder.SIZE_CONTENT_OUTPUT_TOTAL.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "")
                        .set(processorStatus.getOutputBytes());
                nifiMetricsHolder.SIZE_CONTENT_INPUT_TOTAL.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "")
                        .set(processorStatus.getInputBytes());

                nifiMetricsHolder.AMOUNT_ITEMS_OUTPUT.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "")
                        .set(processorStatus.getOutputCount());
                nifiMetricsHolder.AMOUNT_ITEMS_INPUT.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "")
                        .set(processorStatus.getInputCount());

                nifiMetricsHolder.AVERAGE_LINEAGE_DURATION.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "")
                        .set(processorStatus.getAverageLineageDuration());

                nifiMetricsHolder.AMOUNT_THREADS_TOTAL_ACTIVE.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId)
                        .set(status.getActiveThreadCount() == null ? 0 : status.getActiveThreadCount());
                nifiMetricsHolder.AMOUNT_THREADS_TOTAL_TERMINATED.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId)
                        .set(status.getTerminatedThreadCount() == null ? 0 : status.getTerminatedThreadCount());

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
                nifiMetricsHolder.SIZE_CONTENT_OUTPUT_TOTAL.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getOutputBytes());
                nifiMetricsHolder.SIZE_CONTENT_INPUT_TOTAL.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getInputBytes());
                nifiMetricsHolder.SIZE_CONTENT_QUEUED_TOTAL.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getQueuedBytes());

                nifiMetricsHolder.AMOUNT_ITEMS_OUTPUT.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getOutputCount());
                nifiMetricsHolder.AMOUNT_ITEMS_INPUT.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getInputCount());
                nifiMetricsHolder.AMOUNT_ITEMS_QUEUED.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getQueuedCount());

                nifiMetricsHolder.BACKPRESSURE_BYTES_THRESHOLD.labels(
                        instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getBackPressureBytesThreshold());
                nifiMetricsHolder.BACKPRESSURE_OBJECT_THRESHOLD.labels(
                        instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getBackPressureObjectThreshold());
                boolean isBackpressureEnabled = (connectionStatus.getBackPressureObjectThreshold() > 0 && connectionStatus.getBackPressureObjectThreshold() <= connectionStatus.getQueuedCount())
                        || (connectionStatus.getBackPressureBytesThreshold() > 0 && connectionStatus.getBackPressureBytesThreshold() <= connectionStatus.getMaxQueuedBytes());
                nifiMetricsHolder.IS_BACKPRESSURE_ENABLED.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(isBackpressureEnabled ? 1 : 0);
            }
            for (PortStatus portStatus : status.getInputPortStatus()) {
                final String portComponentId = StringUtils.isEmpty(portStatus.getId()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String portComponentName = StringUtils.isEmpty(portStatus.getName()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String parentId = StringUtils.isEmpty(portStatus.getGroupId()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String portComponentType = "InputPort";
                nifiMetricsHolder.AMOUNT_FLOWFILES_SENT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getFlowFilesSent());
                nifiMetricsHolder.AMOUNT_FLOWFILES_RECEIVED.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getFlowFilesReceived());

                nifiMetricsHolder.AMOUNT_BYTES_SENT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getBytesSent());
                nifiMetricsHolder.AMOUNT_BYTES_READ.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getInputBytes());
                nifiMetricsHolder.AMOUNT_BYTES_WRITTEN.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getOutputBytes());
                nifiMetricsHolder.TOTAL_BYTES_READ.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).inc(status.getBytesRead());
                nifiMetricsHolder.TOTAL_BYTES_WRITTEN.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).inc(status.getBytesWritten());
                nifiMetricsHolder.AMOUNT_BYTES_RECEIVED.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getBytesReceived());

                nifiMetricsHolder.AMOUNT_ITEMS_OUTPUT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "")
                        .set(portStatus.getOutputCount());
                nifiMetricsHolder.AMOUNT_ITEMS_INPUT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "")
                        .set(portStatus.getInputCount());

                final Boolean isTransmitting = portStatus.isTransmitting();
                nifiMetricsHolder.IS_TRANSMITTING.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, portStatus.getRunStatus().name())
                        .set(isTransmitting == null ? 0 : (isTransmitting ? 1 : 0));

                nifiMetricsHolder.AMOUNT_THREADS_TOTAL_ACTIVE.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getActiveThreadCount());
            }
            for (PortStatus portStatus : status.getOutputPortStatus()) {
                final String portComponentId = StringUtils.isEmpty(portStatus.getId()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String portComponentName = StringUtils.isEmpty(portStatus.getName()) ? DEFAULT_LABEL_STRING : portStatus.getName();
                final String parentId = StringUtils.isEmpty(portStatus.getGroupId()) ? DEFAULT_LABEL_STRING : portStatus.getGroupId();
                final String portComponentType = "OutputPort";
                nifiMetricsHolder.AMOUNT_FLOWFILES_SENT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getFlowFilesSent());
                nifiMetricsHolder.AMOUNT_FLOWFILES_RECEIVED.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getFlowFilesReceived());

                nifiMetricsHolder.AMOUNT_BYTES_SENT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getBytesSent());
                nifiMetricsHolder.AMOUNT_BYTES_READ.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getInputBytes());
                nifiMetricsHolder.AMOUNT_BYTES_WRITTEN.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getOutputBytes());
                nifiMetricsHolder.TOTAL_BYTES_READ.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).inc(status.getBytesRead());
                nifiMetricsHolder.TOTAL_BYTES_WRITTEN.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).inc(status.getBytesWritten());
                nifiMetricsHolder.AMOUNT_BYTES_RECEIVED.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getBytesReceived());

                nifiMetricsHolder.AMOUNT_ITEMS_OUTPUT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "")
                        .set(portStatus.getOutputCount());
                nifiMetricsHolder.AMOUNT_ITEMS_INPUT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "")
                        .set(portStatus.getInputCount());

                final Boolean isTransmitting = portStatus.isTransmitting();
                nifiMetricsHolder.IS_TRANSMITTING.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, portStatus.getRunStatus().name())
                        .set(isTransmitting == null ? 0 : (isTransmitting ? 1 : 0));

                nifiMetricsHolder.AMOUNT_THREADS_TOTAL_ACTIVE.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getActiveThreadCount());
            }
            for (RemoteProcessGroupStatus remoteProcessGroupStatus : status.getRemoteProcessGroupStatus()) {
                final String rpgComponentId = StringUtils.isEmpty(remoteProcessGroupStatus.getId()) ? DEFAULT_LABEL_STRING : remoteProcessGroupStatus.getId();
                final String rpgComponentName = StringUtils.isEmpty(remoteProcessGroupStatus.getName()) ? DEFAULT_LABEL_STRING : remoteProcessGroupStatus.getName();
                final String parentId = StringUtils.isEmpty(remoteProcessGroupStatus.getGroupId()) ? DEFAULT_LABEL_STRING : remoteProcessGroupStatus.getGroupId();
                final String rpgComponentType = "RemoteProcessGroup";

                nifiMetricsHolder.AMOUNT_BYTES_WRITTEN.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId).set(remoteProcessGroupStatus.getSentContentSize());
                nifiMetricsHolder.AMOUNT_BYTES_RECEIVED.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId).set(remoteProcessGroupStatus.getReceivedContentSize());

                nifiMetricsHolder.AMOUNT_ITEMS_OUTPUT.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "")
                        .set(remoteProcessGroupStatus.getSentCount());
                nifiMetricsHolder.AMOUNT_ITEMS_INPUT.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "")
                        .set(remoteProcessGroupStatus.getReceivedCount());

                nifiMetricsHolder.ACTIVE_REMOTE_PORT_COUNT.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "")
                        .set(remoteProcessGroupStatus.getActiveRemotePortCount());
                nifiMetricsHolder.INACTIVE_REMOTE_PORT_COUNT.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "")
                        .set(remoteProcessGroupStatus.getInactiveRemotePortCount());

                nifiMetricsHolder.AVERAGE_LINEAGE_DURATION.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "")
                        .set(remoteProcessGroupStatus.getAverageLineageDuration());

                nifiMetricsHolder.IS_TRANSMITTING.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, remoteProcessGroupStatus.getTransmissionStatus().name())
                        .set(TransmissionStatus.Transmitting.equals(remoteProcessGroupStatus.getTransmissionStatus()) ? 1 : 0);

                nifiMetricsHolder.AMOUNT_THREADS_TOTAL_ACTIVE.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId).set(remoteProcessGroupStatus.getActiveThreadCount());
            }
        }

        return nifiMetricsHolder.getRegistry();
    }

    public static CollectorRegistry createJvmMetrics(JvmMetricsHolder jvmMetricsHolder, JvmMetrics jvmMetrics, String instId) {
        final String instanceId = StringUtils.isEmpty(instId) ? DEFAULT_LABEL_STRING : instId;
        jvmMetricsHolder.JVM_HEAP_USED.labels(instanceId).set(jvmMetrics.heapUsed(DataUnit.B));
        jvmMetricsHolder.JVM_HEAP_USAGE.labels(instanceId).set(jvmMetrics.heapUsage());
        jvmMetricsHolder.JVM_HEAP_NON_USAGE.labels(instanceId).set(jvmMetrics.nonHeapUsage());

        jvmMetricsHolder.JVM_THREAD_COUNT.labels(instanceId).set(jvmMetrics.threadCount());
        jvmMetricsHolder.JVM_DAEMON_THREAD_COUNT.labels(instanceId).set(jvmMetrics.daemonThreadCount());

        jvmMetricsHolder.JVM_UPTIME.labels(instanceId).set(jvmMetrics.uptime());
        jvmMetricsHolder.JVM_FILE_DESCRIPTOR_USAGE.labels(instanceId).set(jvmMetrics.fileDescriptorUsage());

        jvmMetrics.garbageCollectors()
                .forEach((name, stat) -> {
                    jvmMetricsHolder.JVM_GC_RUNS.labels(instanceId, name).set(stat.getRuns());
                    jvmMetricsHolder.JVM_GC_TIME.labels(instanceId, name).set(stat.getTime(TimeUnit.MILLISECONDS));
                });

        return jvmMetricsHolder.getRegistry();
    }

    public static CollectorRegistry createConnectionStatusAnalyticsMetrics(ConnectionAnalyticsMetricsHolder connectionAnalyticsMetricsHolder, StatusAnalytics statusAnalytics,
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
            connectionAnalyticsMetricsHolder.TIME_TO_BYTES_BACKPRESSURE_PREDICTION.labels(
                    instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                    .set(predictions.get("timeToBytesBackpressureMillis"));
            connectionAnalyticsMetricsHolder.TIME_TO_COUNT_BACKPRESSURE_PREDICTION.labels(
                    instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                    .set(predictions.get("timeToCountBackpressureMillis"));
            connectionAnalyticsMetricsHolder.BYTES_AT_NEXT_INTERVAL_PREDICTION.labels(
                    instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                    .set(predictions.get("nextIntervalBytes"));
            connectionAnalyticsMetricsHolder.COUNT_AT_NEXT_INTERVAL_PREDICTION.labels(
                    instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                    .set(predictions.get("nextIntervalCount"));
        }

        return connectionAnalyticsMetricsHolder.getRegistry();
    }

    public static CollectorRegistry createBulletinMetrics(BulletinMetricsHolder bulletinMetricsHolder, String instId, String compType, String compId, String pgId, String nodeAddr,
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
        bulletinMetricsHolder.BULLETIN.labels(instanceId, componentType, componentId, parentId, nodeAddress, category, sourceName, sourceId, level).set(1);
        return bulletinMetricsHolder.getRegistry();
    }
}
