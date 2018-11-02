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
package org.apache.nifi.reporting.gcp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.scheduling.SchedulingStrategy;

import com.google.api.Metric;
import com.google.api.MetricDescriptor;
import com.google.api.MetricDescriptor.MetricKind;
import com.google.api.MetricDescriptor.ValueType;
import com.google.api.MonitoredResource;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.CreateMetricDescriptorRequest;
import com.google.monitoring.v3.CreateTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.monitoring.v3.TypedValue;
import com.google.protobuf.util.Timestamps;
import com.yammer.metrics.core.VirtualMachineMetrics;

@Tags({"reporting", "gcp", "google", "cloud", "metrics"})
@CapabilityDescription("Publishes metrics from NiFi to the Stackdriver Monitoring tool in Google Cloud Platform.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class GCPReportingTask extends AbstractReportingTask {

    private static final String CUSTOM_METRIC_DOMAIN = "custom.googleapis.com";

    public static final PropertyDescriptor PROJECT_ID = new PropertyDescriptor.Builder()
            .name("gcp-project-id")
            .displayName("Project ID")
            .description("Google Cloud Project ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor RESOURCE_TYPE = new PropertyDescriptor.Builder()
            .name("gcp-resource-type")
            .displayName("Resource Type")
            .description("A common practice is to use the monitored resource objects that represent the physical resources where NiFi is running")
            .allowableValues("gce_instance", "k8s_pod", "aws_ec2_instance", "generic_node")
            .defaultValue("gce_instance")
            .required(true)
            .build();

    public static final PropertyDescriptor ZONE = new PropertyDescriptor.Builder()
            .name("gcp-zone")
            .displayName("Zone")
            .description("Zone where is located the resource where NiFi is running")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("gcp-hostname")
            .displayName("Hostname")
            .description("The Hostname of this NiFi instance to be included in the metrics sent to Ambari")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("${hostname(false)}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private MetricServiceClient client;
    private ProjectName name;
    private MonitoredResource resource;
    private List<String> garbageCollect = new ArrayList<String>();
    private volatile VirtualMachineMetrics virtualMachineMetrics;
    private volatile long lastSentBulletinId = -1L;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PROJECT_ID);
        properties.add(RESOURCE_TYPE);
        properties.add(ZONE);
        properties.add(HOSTNAME);
        return properties;
    }

    @OnScheduled
    public void setup(final ConfigurationContext context) throws IOException {
        client = MetricServiceClient.create();
        name = ProjectName.of(context.getProperty(PROJECT_ID).evaluateAttributeExpressions().getValue());
        virtualMachineMetrics = VirtualMachineMetrics.getInstance();

        for(GCPMetric metric : GCPMetrics.INT_METRICS) {
            addMetricDescriptor(metric.getId(), metric.getDescription(), MetricDescriptor.MetricKind.GAUGE, MetricDescriptor.ValueType.INT64);
        }
        for(GCPMetric metric : GCPMetrics.DOUBLE_METRICS) {
            addMetricDescriptor(metric.getId(), metric.getDescription(), MetricDescriptor.MetricKind.GAUGE, MetricDescriptor.ValueType.DOUBLE);
        }

        for (Map.Entry<String,VirtualMachineMetrics.GarbageCollectorStats> entry : virtualMachineMetrics.garbageCollectors().entrySet()) {
            final String gcName = entry.getKey().replace(" ", "");
            addMetricDescriptor(GCPMetrics.JVM_GC_RUNS.getId() + gcName, GCPMetrics.JVM_GC_RUNS.getDescription() + gcName, MetricDescriptor.MetricKind.GAUGE, MetricDescriptor.ValueType.INT64);
            addMetricDescriptor(GCPMetrics.JVM_GC_TIME.getId() + gcName, GCPMetrics.JVM_GC_TIME.getDescription() + gcName, MetricDescriptor.MetricKind.GAUGE, MetricDescriptor.ValueType.INT64);
            garbageCollect.add(gcName);
        }

        // Prepares the monitored resource descriptor
        Map<String, String> resourceLabels = new HashMap<>();
        resourceLabels.put("instance_id", context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue());
        resourceLabels.put("zone", context.getProperty(ZONE).evaluateAttributeExpressions().getValue());

        resource = MonitoredResource.newBuilder()
                .setType(context.getProperty(RESOURCE_TYPE).getValue())
                .putAllLabels(resourceLabels)
                .build();
    }

    private void addMetricDescriptor(String metricId, String metricDescription, MetricKind metricKind, ValueType metricValueType) {
        getLogger().debug("Adding metric descriptor:" + CUSTOM_METRIC_DOMAIN + "/" + metricId);
        MetricDescriptor descriptor = MetricDescriptor.newBuilder()
                .setType(CUSTOM_METRIC_DOMAIN + "/" + metricId)
                .setDescription(metricDescription)
                .setMetricKind(metricKind)
                .setValueType(metricValueType)
                .build();

        CreateMetricDescriptorRequest request = CreateMetricDescriptorRequest.newBuilder()
                .setName(name.toString())
                .setMetricDescriptor(descriptor)
                .build();

        client.createMetricDescriptor(request);
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        try {
            final ProcessGroupStatus status = context.getEventAccess().getControllerStatus();
            final long now = System.currentTimeMillis();

            final BulletinQuery bulletinQuery = new BulletinQuery.Builder().after(lastSentBulletinId).build();
            final List<Bulletin> bulletins = context.getBulletinRepository().findBulletins(bulletinQuery);
            final OptionalLong opMaxId = bulletins.stream().mapToLong(t -> t.getId()).max();
            final Long currMaxId = opMaxId.isPresent() ? opMaxId.getAsLong() : -1;

            if(currMaxId < lastSentBulletinId){
                lastSentBulletinId = -1;
            }
            lastSentBulletinId = currMaxId;

            final long durationNanos = calculateProcessingNanos(status);
            final long durationSeconds = TimeUnit.SECONDS.convert(durationNanos, TimeUnit.NANOSECONDS);

            List<TimeSeries> timeSeriesList = new ArrayList<>();
            timeSeriesList.add(createTimeSeries(GCPMetrics.ACTIVE_THREADS, now, TypedValue.newBuilder().setInt64Value(status.getActiveThreadCount()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.BULLETINS, now, TypedValue.newBuilder().setInt64Value(bulletins.size()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_HEAP_USED, now, TypedValue.newBuilder().setDoubleValue(virtualMachineMetrics.heapUsed()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_HEAP_USAGE, now, TypedValue.newBuilder().setDoubleValue(virtualMachineMetrics.heapUsage()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_NON_HEAP_USAGE, now, TypedValue.newBuilder().setDoubleValue(virtualMachineMetrics.nonHeapUsage()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_FILE_DESCRIPTOR, now, TypedValue.newBuilder().setDoubleValue(virtualMachineMetrics.fileDescriptorUsage()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.BYTES_QUEUED, now, TypedValue.newBuilder().setInt64Value(status.getQueuedContentSize()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.BYTES_READ, now, TypedValue.newBuilder().setInt64Value(status.getBytesRead()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.BYTES_RECEIVED, now, TypedValue.newBuilder().setInt64Value(status.getBytesReceived()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.BYTES_SENT, now, TypedValue.newBuilder().setInt64Value(status.getBytesSent()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.BYTES_WRITTEN, now, TypedValue.newBuilder().setInt64Value(status.getBytesWritten()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.FF_QUEUED, now, TypedValue.newBuilder().setInt64Value(status.getQueuedCount()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.FF_RECEIVED, now, TypedValue.newBuilder().setInt64Value(status.getFlowFilesReceived()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.FF_SENT, now, TypedValue.newBuilder().setInt64Value(status.getFlowFilesSent()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_DAEMON_THREAD_COUNT, now, TypedValue.newBuilder().setInt64Value(virtualMachineMetrics.daemonThreadCount()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_THREAD_COUNT, now, TypedValue.newBuilder().setInt64Value(virtualMachineMetrics.threadCount()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_UPTIME, now, TypedValue.newBuilder().setInt64Value(virtualMachineMetrics.uptime()).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.TASK_DURATION_NANOSEC, now, TypedValue.newBuilder().setInt64Value(durationNanos).build()));
            timeSeriesList.add(createTimeSeries(GCPMetrics.TASK_DURATION_SEC, now, TypedValue.newBuilder().setInt64Value(durationSeconds).build()));

            for (Map.Entry<Thread.State,Double> entry : virtualMachineMetrics.threadStatePercentages().entrySet()) {
                final int normalizedValue = (int) (100 * (entry.getValue() == null ? 0 : entry.getValue()));
                switch(entry.getKey()) {
                    case BLOCKED:
                        timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_THREAD_STATES_BLOCKED, now, TypedValue.newBuilder().setInt64Value(normalizedValue).build()));
                        break;
                    case RUNNABLE:
                        timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_THREAD_STATES_RUNNABLE, now, TypedValue.newBuilder().setInt64Value(normalizedValue).build()));
                        break;
                    case TERMINATED:
                        timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_THREAD_STATES_TERMINATED, now, TypedValue.newBuilder().setInt64Value(normalizedValue).build()));
                        break;
                    case TIMED_WAITING:
                        timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_THREAD_STATES_TIMED_WAITING, now, TypedValue.newBuilder().setInt64Value(normalizedValue).build()));
                        break;
                    default:
                        break;
                }
            }

            for (Map.Entry<String,VirtualMachineMetrics.GarbageCollectorStats> entry : virtualMachineMetrics.garbageCollectors().entrySet()) {
                final String gcName = entry.getKey().replace(" ", "");

                if(!garbageCollect.contains(gcName)) {
                    // this is required in case the descriptor has not been created during the @OnScheduled call
                    addMetricDescriptor(GCPMetrics.JVM_GC_RUNS.getId() + gcName, GCPMetrics.JVM_GC_RUNS.getDescription() + gcName, MetricDescriptor.MetricKind.GAUGE, MetricDescriptor.ValueType.INT64);
                    addMetricDescriptor(GCPMetrics.JVM_GC_TIME.getId() + gcName, GCPMetrics.JVM_GC_TIME.getDescription() + gcName, MetricDescriptor.MetricKind.GAUGE, MetricDescriptor.ValueType.INT64);
                    garbageCollect.add(gcName);
                }


                final long runs = entry.getValue().getRuns();
                final long timeMS = entry.getValue().getTime(TimeUnit.MILLISECONDS);
                timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_GC_RUNS.getId() + gcName, now, TypedValue.newBuilder().setInt64Value(runs).build()));
                timeSeriesList.add(createTimeSeries(GCPMetrics.JVM_GC_TIME.getId() + gcName, now, TypedValue.newBuilder().setInt64Value(timeMS).build()));
            }

            CreateTimeSeriesRequest request = CreateTimeSeriesRequest.newBuilder()
                    .setName(name.toString())
                    .addAllTimeSeries(timeSeriesList)
                    .build();

            // Writes time series data
            client.createTimeSeries(request);
        } catch (Exception e) {
            getLogger().error("Exception", e);
            throw e;
        }
    }

    private TimeSeries createTimeSeries(String gcpMetricId, long now, TypedValue typedValue) {
        TimeInterval interval = TimeInterval.newBuilder().setEndTime(Timestamps.fromMillis(now)).build();
        Point point = Point.newBuilder().setInterval(interval).setValue(typedValue).build();

        List<Point> pointList = new ArrayList<>();
        pointList.add(point);

        Metric metric = Metric.newBuilder()
                .setType(CUSTOM_METRIC_DOMAIN + "/" + gcpMetricId)
                .build();

        return TimeSeries.newBuilder()
                .setMetric(metric)
                .setResource(resource)
                .addAllPoints(pointList)
                .build();
    }

    private TimeSeries createTimeSeries(GCPMetric gcpMetric, long now, TypedValue typedValue) {
        return createTimeSeries(gcpMetric.getId(), now, typedValue);
    }

    private long calculateProcessingNanos(final ProcessGroupStatus status) {
        long nanos = 0L;

        for (final ProcessorStatus procStats : status.getProcessorStatus()) {
            nanos += procStats.getProcessingNanos();
        }

        for (final ProcessGroupStatus childGroupStatus : status.getProcessGroupStatus()) {
            nanos += calculateProcessingNanos(childGroupStatus);
        }

        return nanos;
    }

}
