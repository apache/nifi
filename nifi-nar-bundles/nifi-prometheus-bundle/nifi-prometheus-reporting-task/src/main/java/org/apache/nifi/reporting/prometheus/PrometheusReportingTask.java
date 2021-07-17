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

package org.apache.nifi.reporting.prometheus;

import io.prometheus.client.CollectorRegistry;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.metrics.jvm.JmxJvmMetrics;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.prometheus.util.JvmMetricsRegistry;
import org.apache.nifi.prometheus.util.NiFiMetricsRegistry;
import org.apache.nifi.prometheus.util.PrometheusMetricsUtil;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.StringUtils;
import org.eclipse.jetty.server.Server;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.nifi.prometheus.util.PrometheusMetricsUtil.METRICS_STRATEGY_COMPONENTS;
import static org.apache.nifi.prometheus.util.PrometheusMetricsUtil.METRICS_STRATEGY_PG;
import static org.apache.nifi.prometheus.util.PrometheusMetricsUtil.METRICS_STRATEGY_ROOT;

@Tags({ "reporting", "prometheus", "metrics", "time series data" })
@CapabilityDescription("Reports metrics in Prometheus format by creating a /metrics HTTP(S) endpoint which can be used for external monitoring of the application."
        + " The reporting task reports a set of metrics regarding the JVM (optional) and the NiFi instance. Note that if the underlying Jetty server (i.e. the "
        + "Prometheus endpoint) cannot be started (for example if two PrometheusReportingTask instances are started on the same port), this may cause a delay in "
        + "shutting down NiFi while it waits for the server resources to be cleaned up.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "60 sec")
public class PrometheusReportingTask extends AbstractReportingTask {

    private PrometheusServer prometheusServer;

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-ssl-context")
            .displayName("SSL Context Service")
            .description("The SSL Context Service to use in order to secure the server. If specified, the server will"
                    + "accept only HTTPS requests; otherwise, the server will accept only HTTP requests")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();

    public static final PropertyDescriptor METRICS_STRATEGY = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-metrics-strategy")
            .displayName("Metrics Reporting Strategy")
            .description("The granularity on which to report metrics. Options include only the root process group, all process groups, or all components")
            .allowableValues(METRICS_STRATEGY_ROOT, METRICS_STRATEGY_PG, METRICS_STRATEGY_COMPONENTS)
            .defaultValue(METRICS_STRATEGY_COMPONENTS.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor SEND_JVM_METRICS = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-metrics-send-jvm")
            .displayName("Send JVM metrics")
            .description("Send JVM metrics in addition to the NiFi metrics")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PrometheusMetricsUtil.METRICS_ENDPOINT_PORT);
        props.add(PrometheusMetricsUtil.INSTANCE_ID);
        props.add(METRICS_STRATEGY);
        props.add(SEND_JVM_METRICS);
        props.add(SSL_CONTEXT);
        props.add(PrometheusMetricsUtil.CLIENT_AUTH);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) {
        SSLContextService sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        final String metricsEndpointPort = context.getProperty(PrometheusMetricsUtil.METRICS_ENDPOINT_PORT).evaluateAttributeExpressions().getValue();

        try {
            List<Function<ReportingContext, CollectorRegistry>> metricsCollectors = new ArrayList<>();
            if (sslContextService == null) {
                this.prometheusServer = new PrometheusServer(new InetSocketAddress(Integer.parseInt(metricsEndpointPort)), getLogger());
            } else {
                final String clientAuthValue = context.getProperty(PrometheusMetricsUtil.CLIENT_AUTH).getValue();
                final boolean need;
                final boolean want;
                if (PrometheusMetricsUtil.CLIENT_NEED.getValue().equals(clientAuthValue)) {
                    need = true;
                    want = false;
                } else if (PrometheusMetricsUtil.CLIENT_WANT.getValue().equals(clientAuthValue)) {
                    need = false;
                    want = true;
                } else {
                    need = false;
                    want = false;
                }
                this.prometheusServer = new PrometheusServer(Integer.parseInt(metricsEndpointPort), sslContextService, getLogger(), need, want);
            }
            Function<ReportingContext, CollectorRegistry> nifiMetrics = (reportingContext) -> {
                EventAccess eventAccess = reportingContext.getEventAccess();
                ProcessGroupStatus rootGroupStatus = eventAccess.getControllerStatus();
                String instanceId = reportingContext.getProperty(PrometheusMetricsUtil.INSTANCE_ID).evaluateAttributeExpressions().getValue();
                if (instanceId == null) {
                    instanceId = "";
                }
                String metricsStrategy = reportingContext.getProperty(METRICS_STRATEGY).getValue();
                NiFiMetricsRegistry nifiMetricsRegistry = new NiFiMetricsRegistry();
                CollectorRegistry collectorRegistry = PrometheusMetricsUtil.createNifiMetrics(nifiMetricsRegistry, rootGroupStatus, instanceId, "", "RootProcessGroup", metricsStrategy);
                // Add the total byte counts (read/written) to the NiFi metrics registry
                final String rootPGId = StringUtils.isEmpty(rootGroupStatus.getId()) ? "" : rootGroupStatus.getId();
                final String rootPGName = StringUtils.isEmpty(rootGroupStatus.getName()) ? "" : rootGroupStatus.getName();
                nifiMetricsRegistry.setDataPoint(eventAccess.getTotalBytesRead(), "TOTAL_BYTES_READ",
                        instanceId, "RootProcessGroup", rootPGName, rootPGId, "");
                nifiMetricsRegistry.setDataPoint(eventAccess.getTotalBytesWritten(), "TOTAL_BYTES_WRITTEN",
                        instanceId, "RootProcessGroup", rootPGName, rootPGId, "");
                nifiMetricsRegistry.setDataPoint(eventAccess.getTotalBytesSent(), "TOTAL_BYTES_SENT",
                        instanceId, "RootProcessGroup", rootPGName, rootPGId, "");
                nifiMetricsRegistry.setDataPoint(eventAccess.getTotalBytesReceived(), "TOTAL_BYTES_RECEIVED",
                        instanceId, "RootProcessGroup", rootPGName, rootPGId, "");

                return collectorRegistry;
            };
            metricsCollectors.add(nifiMetrics);
            if (context.getProperty(SEND_JVM_METRICS).asBoolean()) {
                Function<ReportingContext, CollectorRegistry> jvmMetrics = (reportingContext) -> {
                    String instanceId = reportingContext.getProperty(PrometheusMetricsUtil.INSTANCE_ID).evaluateAttributeExpressions().getValue();
                    JvmMetricsRegistry jvmMetricsRegistry = new JvmMetricsRegistry();
                    return PrometheusMetricsUtil.createJvmMetrics(jvmMetricsRegistry, JmxJvmMetrics.getInstance(), instanceId);
                };
                metricsCollectors.add(jvmMetrics);
            }
            this.prometheusServer.setMetricsCollectors(metricsCollectors);
            getLogger().info("Started Jetty server");
        } catch (Exception e) {
            // Don't allow this to finish successfully, onTrigger should not be called if the Jetty server wasn't started
            throw new ProcessException("Failed to start Jetty server", e);
        }
    }

    @OnStopped
    public void OnStopped() throws Exception {
        if (prometheusServer != null) {
            Server server = prometheusServer.getServer();
            if (server != null) {
                server.stop();
            }
        }
    }

    @OnShutdown
    public void onShutDown() throws Exception {
        if (prometheusServer != null) {
            Server server = prometheusServer.getServer();
            if (server != null) {
                server.stop();
            }
        }
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        if (prometheusServer != null) {
            prometheusServer.setReportingContext(context);
        }
    }
}
