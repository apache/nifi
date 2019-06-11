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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.AbstractReportingTask;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.eclipse.jetty.server.Server;

import static org.apache.nifi.reporting.prometheus.api.PrometheusMetricsUtil.METRICS_STRATEGY_COMPONENTS;
import static org.apache.nifi.reporting.prometheus.api.PrometheusMetricsUtil.METRICS_STRATEGY_PG;
import static org.apache.nifi.reporting.prometheus.api.PrometheusMetricsUtil.METRICS_STRATEGY_ROOT;

@Tags({ "reporting", "prometheus", "metrics", "time series data" })
@CapabilityDescription("Reports metrics in Prometheus format by creating /metrics http endpoint which can be used for external monitoring of the application."
        + " The reporting task reports a set of metrics regarding the JVM (optional) and the NiFi instance")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "60 sec")

public class PrometheusReportingTask extends AbstractReportingTask {

    private PrometheusServer prometheusServer;
    private SSLContextService sslContextService;

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

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-ssl-context")
            .displayName("SSL Context Service")
            .description("The SSL Context Service to use in order to secure the server. If specified, the server will"
                    + "accept only HTTPS requests; otherwise, the server will accept only HTTP requests")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
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

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(METRICS_ENDPOINT_PORT);
        props.add(INSTANCE_ID);
        props.add(METRICS_STRATEGY);
        props.add(SEND_JVM_METRICS);
        props.add(SSL_CONTEXT);
        props.add(CLIENT_AUTH);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) {
        sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        final String metricsEndpointPort = context.getProperty(METRICS_ENDPOINT_PORT).getValue();

        try {
            if (sslContextService == null) {
                this.prometheusServer = new PrometheusServer(new InetSocketAddress(Integer.parseInt(metricsEndpointPort)), getLogger());
            } else {
                final String clientAuthValue = context.getProperty(CLIENT_AUTH).getValue();
                final boolean need;
                final boolean want;
                if (CLIENT_NEED.equals(clientAuthValue)) {
                    need = true;
                    want = false;
                } else if (CLIENT_WANT.equals(clientAuthValue)) {
                    need = false;
                    want = true;
                } else {
                    need = false;
                    want = false;
                }
                this.prometheusServer = new PrometheusServer(Integer.parseInt(metricsEndpointPort),sslContextService, getLogger(), need, want);
            }
            this.prometheusServer.setInstanceId(context.getProperty(INSTANCE_ID).evaluateAttributeExpressions().getValue());
            this.prometheusServer.setSendJvmMetrics(context.getProperty(SEND_JVM_METRICS).asBoolean());
            getLogger().info("Started JETTY server");
        } catch (Exception e) {
            getLogger().error("Failed to start Jetty server", e);
        }
    }

    @OnStopped
    public void OnStopped() throws Exception {
        Server server = this.prometheusServer.getServer();
        server.stop();
    }

    @OnShutdown
    public void onShutDown() throws Exception {
        Server server = prometheusServer.getServer();
        server.stop();
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        this.prometheusServer.setReportingContext(context);
        this.prometheusServer.setMetricsStrategy(context.getProperty(METRICS_STRATEGY).getValue());
    }
}
