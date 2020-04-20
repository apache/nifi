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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.prometheus.client.Counter;
import io.prometheus.client.SimpleCollector;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
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

    private static final CollectorRegistry NIFI_REGISTRY = new CollectorRegistry();
    private static final CollectorRegistry JVM_REGISTRY = new CollectorRegistry();
    private static final CollectorRegistry CONNECTION_ANALYTICS_REGISTRY = new CollectorRegistry();
    private static final CollectorRegistry BULLETIN_REGISTRY = new CollectorRegistry();

    public static final Collection<CollectorRegistry> ALL_REGISTRIES = Arrays.asList(NIFI_REGISTRY, CONNECTION_ANALYTICS_REGISTRY, BULLETIN_REGISTRY, JVM_REGISTRY);

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

    // Processor / Process Group metrics
    private static final Gauge AMOUNT_FLOWFILES_SENT = Gauge.build()
            .name("nifi_amount_flowfiles_sent")
            .help("Total number of FlowFiles sent by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_FLOWFILES_TRANSFERRED = Gauge.build()
            .name("nifi_amount_flowfiles_transferred")
            .help("Total number of FlowFiles transferred by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_FLOWFILES_RECEIVED = Gauge.build()
            .name("nifi_amount_flowfiles_received")
            .help("Total number of FlowFiles received by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_FLOWFILES_REMOVED = Gauge.build()
            .name("nifi_amount_flowfiles_removed")
            .help("Total number of FlowFiles removed by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_BYTES_SENT = Gauge.build()
            .name("nifi_amount_bytes_sent")
            .help("Total number of bytes sent by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_BYTES_READ = Gauge.build()
            .name("nifi_amount_bytes_read")
            .help("Total number of bytes read by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Counter TOTAL_BYTES_READ = Counter.build().name("nifi_total_bytes_read")
            .help("Total number of bytes read by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Counter TOTAL_BYTES_WRITTEN = Counter.build().name("nifi_total_bytes_written")
            .help("Total number of bytes written by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_BYTES_WRITTEN = Gauge.build()
            .name("nifi_amount_bytes_written")
            .help("Total number of bytes written by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_BYTES_RECEIVED = Gauge.build()
            .name("nifi_amount_bytes_received")
            .help("Total number of bytes received by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_BYTES_TRANSFERRED = Gauge.build()
            .name("nifi_amount_bytes_transferred")
            .help("Total number of Bytes transferred by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_THREADS_TOTAL_ACTIVE = Gauge.build()
            .name("nifi_amount_threads_active")
            .help("Total number of threads active for the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_THREADS_TOTAL_TERMINATED = Gauge.build()
            .name("nifi_amount_threads_terminated")
            .help("Total number of threads terminated for the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id")
            .register(NIFI_REGISTRY);

    private static final Gauge SIZE_CONTENT_OUTPUT_TOTAL = Gauge.build()
            .name("nifi_size_content_output_total")
            .help("Total size of content output by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    private static final Gauge SIZE_CONTENT_INPUT_TOTAL = Gauge.build()
            .name("nifi_size_content_input_total")
            .help("Total size of content input by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    private static final Gauge SIZE_CONTENT_QUEUED_TOTAL = Gauge.build()
            .name("nifi_size_content_queued_total")
            .help("Total size of content queued in the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_ITEMS_OUTPUT = Gauge.build()
            .name("nifi_amount_items_output")
            .help("Total number of items output by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_ITEMS_INPUT = Gauge.build()
            .name("nifi_amount_items_input")
            .help("Total number of items input by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    private static final Gauge AMOUNT_ITEMS_QUEUED = Gauge.build()
            .name("nifi_amount_items_queued")
            .help("Total number of items queued by the component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    // Processor metrics
    private static final Gauge PROCESSOR_COUNTERS = Gauge.build()
            .name("nifi_processor_counters")
            .help("Counters exposed by NiFi Processors")
            .labelNames("processor_name", "counter_name", "processor_id", "instance")
            .register(NIFI_REGISTRY);

    // Connection metrics
    private static final Gauge BACKPRESSURE_BYTES_THRESHOLD = Gauge.build()
            .name("nifi_backpressure_bytes_threshold")
            .help("The number of bytes that can be queued before backpressure is applied")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    private static final Gauge BACKPRESSURE_OBJECT_THRESHOLD = Gauge.build()
            .name("nifi_backpressure_object_threshold")
            .help("The number of flow files that can be queued before backpressure is applied")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    private static final Gauge IS_BACKPRESSURE_ENABLED = Gauge.build()
            .name("nifi_backpressure_enabled")
            .help("Whether backpressure has been applied for this component. Values are 0 or 1")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    // Port metrics
    private static final Gauge IS_TRANSMITTING = Gauge.build()
            .name("nifi_transmitting")
            .help("Whether this component is transmitting data. Values are 0 or 1")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id", "run_status")
            .register(NIFI_REGISTRY);

    // Remote Process Group (RPG) metrics
    private static final Gauge ACTIVE_REMOTE_PORT_COUNT = Gauge.build()
            .name("nifi_active_remote_port_count")
            .help("The number of active remote ports associated with this component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    private static final Gauge INACTIVE_REMOTE_PORT_COUNT = Gauge.build()
            .name("nifi_inactive_remote_port_count")
            .help("The number of inactive remote ports associated with this component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    private static final Gauge AVERAGE_LINEAGE_DURATION = Gauge.build()
            .name("nifi_average_lineage_duration")
            .help("The average lineage duration (in milliseconds) for all flow file processed by this component")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(NIFI_REGISTRY);

    // Connection status analytics metrics
    private static final Gauge TIME_TO_BYTES_BACKPRESSURE_PREDICTION = Gauge.build()
            .name("nifi_time_to_bytes_backpressure_prediction")
            .help("Predicted time (in milliseconds) until backpressure will be applied on the connection due to bytes in the queue")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(CONNECTION_ANALYTICS_REGISTRY);

    private static final Gauge TIME_TO_COUNT_BACKPRESSURE_PREDICTION = Gauge.build()
            .name("nifi_time_to_count_backpressure_prediction")
            .help("Predicted time (in milliseconds) until backpressure will be applied on the connection due to number of objects in the queue")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(CONNECTION_ANALYTICS_REGISTRY);

    private static final Gauge BYTES_AT_NEXT_INTERVAL_PREDICTION = Gauge.build()
            .name("nifi_bytes_at_next_interval_prediction")
            .help("Predicted number of bytes in the queue at the next configured interval")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(CONNECTION_ANALYTICS_REGISTRY);

    private static final Gauge COUNT_AT_NEXT_INTERVAL_PREDICTION = Gauge.build()
            .name("nifi_count_at_next_interval_prediction")
            .help("Predicted number of objects in the queue at the next configured interval")
            .labelNames("instance", "component_type", "component_name", "component_id", "parent_id",
                    "source_id", "source_name", "destination_id", "destination_name")
            .register(CONNECTION_ANALYTICS_REGISTRY);

    private static final Gauge BULLETIN = Gauge.build()
            .name("nifi_bulletin")
            .help("Bulletin reported by the NiFi instance")
            .labelNames("instance", "component_type", "component_id", "parent_id",
                    "node_address", "category", "source_name", "source_id", "level")
            .register(BULLETIN_REGISTRY);

    ///////////////////////////////////////////////////////////////
    // JVM Metrics
    ///////////////////////////////////////////////////////////////
    private static final Gauge JVM_HEAP_USED = Gauge.build()
            .name("nifi_jvm_heap_used")
            .help("NiFi JVM heap used")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_HEAP_USAGE = Gauge.build()
            .name("nifi_jvm_heap_usage")
            .help("NiFi JVM heap usage")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_HEAP_NON_USAGE = Gauge.build()
            .name("nifi_jvm_heap_non_usage")
            .help("NiFi JVM heap non usage")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_THREAD_COUNT = Gauge.build()
            .name("nifi_jvm_thread_count")
            .help("NiFi JVM thread count")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_DAEMON_THREAD_COUNT = Gauge.build()
            .name("nifi_jvm_daemon_thread_count")
            .help("NiFi JVM daemon thread count")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_UPTIME = Gauge.build()
            .name("nifi_jvm_uptime")
            .help("NiFi JVM uptime")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_FILE_DESCRIPTOR_USAGE = Gauge.build()
            .name("nifi_jvm_file_descriptor_usage")
            .help("NiFi JVM file descriptor usage")
            .labelNames("instance")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_GC_RUNS = Gauge.build()
            .name("nifi_jvm_gc_runs")
            .help("NiFi JVM GC number of runs")
            .labelNames("instance", "gc_name")
            .register(JVM_REGISTRY);

    private static final Gauge JVM_GC_TIME = Gauge.build()
            .name("nifi_jvm_gc_time")
            .help("NiFi JVM GC time in milliseconds")
            .labelNames("instance", "gc_name")
            .register(JVM_REGISTRY);

    public static CollectorRegistry createNifiMetrics(ProcessGroupStatus status, String instId, String parentProcessGroupId, String compType, String metricsStrategy) {

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

        AMOUNT_FLOWFILES_SENT.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getFlowFilesSent());
        AMOUNT_FLOWFILES_TRANSFERRED.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getFlowFilesTransferred());
        AMOUNT_FLOWFILES_RECEIVED.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getFlowFilesReceived());

        AMOUNT_BYTES_SENT.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getBytesSent());
        AMOUNT_BYTES_READ.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getBytesRead());
        AMOUNT_BYTES_WRITTEN.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getBytesWritten());
        TOTAL_BYTES_READ.labels(instanceId, componentType, componentName, componentId, parentPGId).inc(status.getBytesRead());
        TOTAL_BYTES_WRITTEN.labels(instanceId, componentType, componentName, componentId, parentPGId).inc(status.getBytesWritten());
        AMOUNT_BYTES_RECEIVED.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getBytesReceived());
        AMOUNT_BYTES_TRANSFERRED.labels(instanceId, componentType, componentName, componentId, parentPGId).set(status.getBytesTransferred());

        SIZE_CONTENT_OUTPUT_TOTAL.labels(instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "")
                .set(status.getOutputContentSize());
        SIZE_CONTENT_INPUT_TOTAL.labels(instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "")
                .set(status.getInputContentSize());
        SIZE_CONTENT_QUEUED_TOTAL.labels(instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "")
                .set(status.getQueuedContentSize());

        AMOUNT_ITEMS_OUTPUT.labels(instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "")
                .set(status.getOutputCount());
        AMOUNT_ITEMS_INPUT.labels(instanceId, componentType, componentName, componentId, parentPGId, "", "", "", "")
                .set(status.getInputCount());
        AMOUNT_ITEMS_QUEUED.labels(instanceId, componentType, componentName, componentId, parentPGId,"", "", "", "")
                .set(status.getQueuedCount());

        AMOUNT_THREADS_TOTAL_ACTIVE.labels(instanceId, componentType, componentName, componentId, parentPGId)
                .set(status.getActiveThreadCount() == null ? 0 : status.getActiveThreadCount());
        AMOUNT_THREADS_TOTAL_TERMINATED.labels(instanceId, componentType, componentName, componentId, parentPGId)
                .set(status.getTerminatedThreadCount() == null ? 0 : status.getTerminatedThreadCount());

        // Report metrics for child process groups if specified
        if (METRICS_STRATEGY_PG.getValue().equals(metricsStrategy) || METRICS_STRATEGY_COMPONENTS.getValue().equals(metricsStrategy)) {
            status.getProcessGroupStatus().forEach((childGroupStatus) -> createNifiMetrics(childGroupStatus, instanceId, componentId, "ProcessGroup", metricsStrategy));
        }

        if (METRICS_STRATEGY_COMPONENTS.getValue().equals(metricsStrategy)) {
            // Report metrics for all components
            for (ProcessorStatus processorStatus : status.getProcessorStatus()) {
                Map<String, Long> counters = processorStatus.getCounters();

                if (counters != null) {
                    counters.entrySet().stream().forEach(entry -> PROCESSOR_COUNTERS
                            .labels(processorStatus.getName(), entry.getKey(), processorStatus.getId(), instanceId).set(entry.getValue()));
                }

                final String procComponentType = "Processor";
                final String procComponentId = StringUtils.isEmpty(processorStatus.getId()) ? DEFAULT_LABEL_STRING : processorStatus.getId();
                final String procComponentName = StringUtils.isEmpty(processorStatus.getName()) ? DEFAULT_LABEL_STRING : processorStatus.getName();
                final String parentId = StringUtils.isEmpty(processorStatus.getGroupId()) ? DEFAULT_LABEL_STRING : processorStatus.getGroupId();

                AMOUNT_FLOWFILES_SENT.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getFlowFilesSent());
                AMOUNT_FLOWFILES_RECEIVED.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getFlowFilesReceived());
                AMOUNT_FLOWFILES_REMOVED.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getFlowFilesRemoved());

                AMOUNT_BYTES_SENT.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getBytesSent());
                AMOUNT_BYTES_READ.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getBytesRead());
                AMOUNT_BYTES_WRITTEN.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getBytesWritten());
                TOTAL_BYTES_READ.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).inc(status.getBytesRead());
                TOTAL_BYTES_WRITTEN.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).inc(status.getBytesWritten());
                AMOUNT_BYTES_RECEIVED.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId).set(processorStatus.getBytesReceived());

                SIZE_CONTENT_OUTPUT_TOTAL.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "")
                        .set(processorStatus.getOutputBytes());
                SIZE_CONTENT_INPUT_TOTAL.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "")
                        .set(processorStatus.getInputBytes());

                AMOUNT_ITEMS_OUTPUT.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "")
                        .set(processorStatus.getOutputCount());
                AMOUNT_ITEMS_INPUT.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "")
                        .set(processorStatus.getInputCount());

                AVERAGE_LINEAGE_DURATION.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId, "", "", "", "")
                        .set(processorStatus.getAverageLineageDuration());

                AMOUNT_THREADS_TOTAL_ACTIVE.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId)
                        .set(status.getActiveThreadCount() == null ? 0 : status.getActiveThreadCount());
                AMOUNT_THREADS_TOTAL_TERMINATED.labels(instanceId, procComponentType, procComponentName, procComponentId, parentId)
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
                SIZE_CONTENT_OUTPUT_TOTAL.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getOutputBytes());
                SIZE_CONTENT_INPUT_TOTAL.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getInputBytes());
                SIZE_CONTENT_QUEUED_TOTAL.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getQueuedBytes());

                AMOUNT_ITEMS_OUTPUT.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getOutputCount());
                AMOUNT_ITEMS_INPUT.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getInputCount());
                AMOUNT_ITEMS_QUEUED.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getQueuedCount());

                BACKPRESSURE_BYTES_THRESHOLD.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getBackPressureBytesThreshold());
                BACKPRESSURE_OBJECT_THRESHOLD.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(connectionStatus.getBackPressureObjectThreshold());
                boolean isBackpressureEnabled = (connectionStatus.getBackPressureObjectThreshold() > 0 && connectionStatus.getBackPressureObjectThreshold() <= connectionStatus.getQueuedCount())
                        || (connectionStatus.getBackPressureBytesThreshold() > 0 && connectionStatus.getBackPressureBytesThreshold() <= connectionStatus.getMaxQueuedBytes());
                IS_BACKPRESSURE_ENABLED.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                        .set(isBackpressureEnabled ? 1 : 0);
            }
            for (PortStatus portStatus : status.getInputPortStatus()) {
                final String portComponentId = StringUtils.isEmpty(portStatus.getId()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String portComponentName = StringUtils.isEmpty(portStatus.getName()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String parentId = StringUtils.isEmpty(portStatus.getGroupId()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String portComponentType = "InputPort";
                AMOUNT_FLOWFILES_SENT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getFlowFilesSent());
                AMOUNT_FLOWFILES_RECEIVED.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getFlowFilesReceived());

                AMOUNT_BYTES_SENT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getBytesSent());
                AMOUNT_BYTES_READ.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getInputBytes());
                AMOUNT_BYTES_WRITTEN.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getOutputBytes());
                TOTAL_BYTES_READ.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).inc(status.getBytesRead());
                TOTAL_BYTES_WRITTEN.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).inc(status.getBytesWritten());
                AMOUNT_BYTES_RECEIVED.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getBytesReceived());

                AMOUNT_ITEMS_OUTPUT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "")
                        .set(portStatus.getOutputCount());
                AMOUNT_ITEMS_INPUT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "")
                        .set(portStatus.getInputCount());

                final Boolean isTransmitting = portStatus.isTransmitting();
                IS_TRANSMITTING.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, portStatus.getRunStatus().name())
                        .set(isTransmitting == null ? 0 : (isTransmitting ? 1 : 0));

                AMOUNT_THREADS_TOTAL_ACTIVE.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getActiveThreadCount());
            }
            for (PortStatus portStatus : status.getOutputPortStatus()) {
                final String portComponentId = StringUtils.isEmpty(portStatus.getId()) ? DEFAULT_LABEL_STRING : portStatus.getId();
                final String portComponentName = StringUtils.isEmpty(portStatus.getName()) ? DEFAULT_LABEL_STRING : portStatus.getName();
                final String parentId = StringUtils.isEmpty(portStatus.getGroupId()) ? DEFAULT_LABEL_STRING : portStatus.getGroupId();
                final String portComponentType = "OutputPort";
                AMOUNT_FLOWFILES_SENT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getFlowFilesSent());
                AMOUNT_FLOWFILES_RECEIVED.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getFlowFilesReceived());

                AMOUNT_BYTES_SENT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getBytesSent());
                AMOUNT_BYTES_READ.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getInputBytes());
                AMOUNT_BYTES_WRITTEN.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getOutputBytes());
                TOTAL_BYTES_READ.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).inc(status.getBytesRead());
                TOTAL_BYTES_WRITTEN.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).inc(status.getBytesWritten());
                AMOUNT_BYTES_RECEIVED.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getBytesReceived());

                AMOUNT_ITEMS_OUTPUT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "")
                        .set(portStatus.getOutputCount());
                AMOUNT_ITEMS_INPUT.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, "", "", "", "")
                        .set(portStatus.getInputCount());

                final Boolean isTransmitting = portStatus.isTransmitting();
                IS_TRANSMITTING.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId, portStatus.getRunStatus().name())
                        .set(isTransmitting == null ? 0 : (isTransmitting ? 1 : 0));

                AMOUNT_THREADS_TOTAL_ACTIVE.labels(instanceId, portComponentType, portComponentName, portComponentId, parentId).set(portStatus.getActiveThreadCount());
            }
            for (RemoteProcessGroupStatus remoteProcessGroupStatus : status.getRemoteProcessGroupStatus()) {
                final String rpgComponentId = StringUtils.isEmpty(remoteProcessGroupStatus.getId()) ? DEFAULT_LABEL_STRING : remoteProcessGroupStatus.getId();
                final String rpgComponentName = StringUtils.isEmpty(remoteProcessGroupStatus.getName()) ? DEFAULT_LABEL_STRING : remoteProcessGroupStatus.getName();
                final String parentId = StringUtils.isEmpty(remoteProcessGroupStatus.getGroupId()) ? DEFAULT_LABEL_STRING : remoteProcessGroupStatus.getGroupId();
                final String rpgComponentType = "RemoteProcessGroup";

                AMOUNT_BYTES_WRITTEN.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId).set(remoteProcessGroupStatus.getSentContentSize());
                AMOUNT_BYTES_RECEIVED.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId).set(remoteProcessGroupStatus.getReceivedContentSize());

                AMOUNT_ITEMS_OUTPUT.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "")
                        .set(remoteProcessGroupStatus.getSentCount());
                AMOUNT_ITEMS_INPUT.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "")
                        .set(remoteProcessGroupStatus.getReceivedCount());

                ACTIVE_REMOTE_PORT_COUNT.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "")
                        .set(remoteProcessGroupStatus.getActiveRemotePortCount());
                INACTIVE_REMOTE_PORT_COUNT.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "")
                        .set(remoteProcessGroupStatus.getInactiveRemotePortCount());

                AVERAGE_LINEAGE_DURATION.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, "", "", "", "")
                        .set(remoteProcessGroupStatus.getAverageLineageDuration());

                IS_TRANSMITTING.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId, remoteProcessGroupStatus.getTransmissionStatus().name())
                        .set(TransmissionStatus.Transmitting.equals(remoteProcessGroupStatus.getTransmissionStatus()) ? 1 : 0);

                AMOUNT_THREADS_TOTAL_ACTIVE.labels(instanceId, rpgComponentType, rpgComponentName, rpgComponentId, parentId).set(remoteProcessGroupStatus.getActiveThreadCount());
            }
        }

        return NIFI_REGISTRY;
    }

    public static CollectorRegistry createJvmMetrics(JvmMetrics jvmMetrics, String instId) {
        final String instanceId = StringUtils.isEmpty(instId) ? DEFAULT_LABEL_STRING : instId;
        JVM_HEAP_USED.labels(instanceId).set(jvmMetrics.heapUsed(DataUnit.B));
        JVM_HEAP_USAGE.labels(instanceId).set(jvmMetrics.heapUsage());
        JVM_HEAP_NON_USAGE.labels(instanceId).set(jvmMetrics.nonHeapUsage());

        JVM_THREAD_COUNT.labels(instanceId).set(jvmMetrics.threadCount());
        JVM_DAEMON_THREAD_COUNT.labels(instanceId).set(jvmMetrics.daemonThreadCount());

        JVM_UPTIME.labels(instanceId).set(jvmMetrics.uptime());
        JVM_FILE_DESCRIPTOR_USAGE.labels(instanceId).set(jvmMetrics.fileDescriptorUsage());

        jvmMetrics.garbageCollectors()
                .forEach((name, stat) -> {
                    JVM_GC_RUNS.labels(instanceId, name).set(stat.getRuns());
                    JVM_GC_TIME.labels(instanceId, name).set(stat.getTime(TimeUnit.MILLISECONDS));
                });

        return JVM_REGISTRY;
    }

    public static CollectorRegistry createConnectionStatusAnalyticsMetrics(StatusAnalytics statusAnalytics, String instId, String connComponentType, String connName,
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
            TIME_TO_BYTES_BACKPRESSURE_PREDICTION.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                    .set(predictions.get("timeToBytesBackpressureMillis"));
            TIME_TO_COUNT_BACKPRESSURE_PREDICTION.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                    .set(predictions.get("timeToCountBackpressureMillis"));
            BYTES_AT_NEXT_INTERVAL_PREDICTION.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                    .set(predictions.get("nextIntervalBytes"));
            COUNT_AT_NEXT_INTERVAL_PREDICTION.labels(instanceId, connComponentType, connComponentName, connComponentId, parentId, sourceId, sourceName, destinationId, destinationName)
                    .set(predictions.get("nextIntervalCount"));
        }

        return CONNECTION_ANALYTICS_REGISTRY;
    }

    public static CollectorRegistry createBulletinMetrics(String instId, String compType, String compId, String pgId, String nodeAddr,
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
        BULLETIN.labels(instanceId, componentType, componentId, parentId, nodeAddress, category, sourceName, sourceId, level).set(1);
        return BULLETIN_REGISTRY;
    }
}
