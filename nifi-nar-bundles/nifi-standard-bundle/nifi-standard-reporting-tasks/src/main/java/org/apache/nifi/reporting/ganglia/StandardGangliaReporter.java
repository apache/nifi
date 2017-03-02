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
package org.apache.nifi.reporting.ganglia;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.GangliaReporter;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.reporting.ReportingContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Configuration of this reporting task requires a "host" property that points
 * to the Ganglia server and optionally allows a "port" property (default
 * otherwise is 8649)
 */
@Tags({"ganglia", "stats"})
@CapabilityDescription("Reports metrics to Ganglia so that Ganglia can be used for external monitoring of the application. Metrics"
        + " reported include JVM Metrics (optional); the following 5-minute NiFi statistics: FlowFiles Received, Bytes Received,"
        + " FlowFiles Sent, Bytes Sent, Bytes Read, Bytes Written, Total Task Duration; and the current values for"
        + " FlowFiles Queued, Bytes Queued, and number of Active Threads.")
public class StandardGangliaReporter extends AbstractReportingTask {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("Hostname")
            .description("The fully-qualified name of the host on which Ganglia is running")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("localhost")
            .build();
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("Port")
            .description("The Port on which Ganglia is listening for incoming connections")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .defaultValue("8649")
            .build();
    public static final PropertyDescriptor SEND_JVM_METRICS = new PropertyDescriptor.Builder()
            .name("Send JVM Metrics")
            .description("Specifies whether or not JVM Metrics should be gathered and sent, in addition to NiFi-specific metrics")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final String METRICS_GROUP = "NiFi";

    private MetricsRegistry metricsRegistry;
    private GangliaReporter gangliaReporter;

    private final AtomicReference<ProcessGroupStatus> latestStatus = new AtomicReference<>();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HOSTNAME);
        properties.add(PORT);
        properties.add(SEND_JVM_METRICS);
        return properties;
    }

    @OnScheduled
    public void onConfigure(final ConfigurationContext config) throws InitializationException {
        metricsRegistry = new MetricsRegistry();

        metricsRegistry.newGauge(new MetricName(METRICS_GROUP, "int32", "FlowFiles Received Last 5 mins"), new Gauge<Integer>() {
            @Override
            public Integer value() {
                final ProcessGroupStatus status = latestStatus.get();
                if (status == null) {
                    return 0;
                }

                final Integer value = status.getFlowFilesReceived();
                return (value == null) ? 0 : value;
            }
        });

        metricsRegistry.newGauge(new MetricName(METRICS_GROUP, "int64", "Bytes Received Last 5 mins"), new Gauge<Long>() {
            @Override
            public Long value() {
                final ProcessGroupStatus status = latestStatus.get();
                if (status == null) {
                    return 0L;
                }

                return status.getBytesReceived();
            }
        });

        metricsRegistry.newGauge(new MetricName(METRICS_GROUP, "int32", "FlowFiles Sent Last 5 mins"), new Gauge<Integer>() {
            @Override
            public Integer value() {
                final ProcessGroupStatus status = latestStatus.get();
                if (status == null) {
                    return 0;
                }

                return status.getFlowFilesSent();
            }
        });

        metricsRegistry.newGauge(new MetricName(METRICS_GROUP, "int64", "Bytes Sent Last 5 mins"), new Gauge<Long>() {
            @Override
            public Long value() {
                final ProcessGroupStatus status = latestStatus.get();
                if (status == null) {
                    return 0L;
                }
                return status.getBytesSent();
            }
        });

        metricsRegistry.newGauge(new MetricName(METRICS_GROUP, "int32", "FlowFiles Queued"), new Gauge<Integer>() {
            @Override
            public Integer value() {
                final ProcessGroupStatus status = latestStatus.get();
                if (status == null) {
                    return 0;
                }

                final Integer value = status.getQueuedCount();
                return (value == null) ? 0 : value;
            }
        });

        metricsRegistry.newGauge(new MetricName(METRICS_GROUP, "int64", "Bytes Queued"), new Gauge<Long>() {
            @Override
            public Long value() {
                final ProcessGroupStatus status = latestStatus.get();
                if (status == null) {
                    return 0L;
                }

                final Long value = status.getQueuedContentSize();
                return (value == null) ? 0L : value;
            }
        });

        metricsRegistry.newGauge(new MetricName(METRICS_GROUP, "int64", "Bytes Read (5 mins)"), new Gauge<Long>() {
            @Override
            public Long value() {
                final ProcessGroupStatus status = latestStatus.get();
                if (status == null) {
                    return 0L;
                }

                final Long value = status.getBytesRead();
                return (value == null) ? 0L : value;
            }
        });

        metricsRegistry.newGauge(new MetricName(METRICS_GROUP, "int64", "Bytes Written (5 mins)"), new Gauge<Long>() {
            @Override
            public Long value() {
                final ProcessGroupStatus status = latestStatus.get();
                if (status == null) {
                    return 0L;
                }

                final Long value = status.getBytesWritten();
                return (value == null) ? 0L : value;
            }
        });

        metricsRegistry.newGauge(new MetricName(METRICS_GROUP, "int32", "Active Threads"), new Gauge<Integer>() {
            @Override
            public Integer value() {
                final ProcessGroupStatus status = latestStatus.get();
                if (status == null) {
                    return 0;
                }

                final Integer value = status.getActiveThreadCount();
                return (value == null) ? 0 : value;
            }
        });

        metricsRegistry.newGauge(new MetricName(METRICS_GROUP, "int32", "Total Task Duration Seconds"), new Gauge<Integer>() {
            @Override
            public Integer value() {
                final ProcessGroupStatus status = latestStatus.get();
                if (status == null) {
                    return 0;
                }

                final long nanos = calculateProcessingNanos(status);
                return (int) TimeUnit.NANOSECONDS.toSeconds(nanos);
            }
        });

        final String gangliaHost = config.getProperty(HOSTNAME).getValue();
        final int port = config.getProperty(PORT).asInteger();

        try {
            gangliaReporter = new GangliaReporter(metricsRegistry, gangliaHost, port, METRICS_GROUP) {
                @Override
                protected String sanitizeName(MetricName name) {
                    return name.getName();
                }
            };

            gangliaReporter.printVMMetrics = config.getProperty(SEND_JVM_METRICS).asBoolean();
        } catch (final IOException e) {
            throw new InitializationException(e);
        }
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final ProcessGroupStatus rootGroupStatus = context.getEventAccess().getControllerStatus();
        this.latestStatus.set(rootGroupStatus);
        gangliaReporter.run();

        getLogger().info("{} Sent metrics to Ganglia", new Object[] {this});
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
