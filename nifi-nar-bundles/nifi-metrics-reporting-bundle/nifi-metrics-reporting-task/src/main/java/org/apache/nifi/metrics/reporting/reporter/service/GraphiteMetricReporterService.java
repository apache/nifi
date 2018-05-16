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
package org.apache.nifi.metrics.reporting.reporter.service;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.graphite.GraphiteSender;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.metrics.reporting.task.MetricsReportingTask;
import org.apache.nifi.processor.util.StandardValidators;

import javax.net.SocketFactory;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A controller service that provides metric reporters for graphite, can be used by {@link MetricsReportingTask}.
 *
 * @author Omer Hadari
 */
@Tags({"metrics", "reporting", "graphite"})
@CapabilityDescription("A controller service that provides metric reporters for graphite. " +
        "Used by MetricsReportingTask.")
public class GraphiteMetricReporterService extends AbstractControllerService implements MetricReporterService {

    /**
     * Points to the hostname of the graphite listener.
     */
    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("host")
            .displayName("Host")
            .description("The hostname of the carbon listener")
            .required(true)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    /**
     * Points to the port on which the graphite server listens.
     */
    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("port")
            .displayName("Port")
            .description("The port on which carbon listens")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    /**
     * Points to the charset name that the graphite server expects.
     */
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("charset")
            .displayName("Charset")
            .description("The charset used by the graphite server")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    /**
     * Prefix for all metric names sent by reporters - for separation of NiFi stats in graphite.
     */
    protected static final PropertyDescriptor METRIC_NAME_PREFIX = new PropertyDescriptor.Builder()
            .name("metric name prefix")
            .displayName("Metric Name Prefix")
            .description("A prefix that will be used for all metric names sent by reporters provided by this service.")
            .required(true)
            .defaultValue("nifi")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    /**
     * List of property descriptors used by the service.
     */
    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HOST);
        props.add(PORT);
        props.add(CHARSET);
        props.add(METRIC_NAME_PREFIX);
        properties = Collections.unmodifiableList(props);
    }

    /**
     * Graphite sender, a connection to the server.
     */
    private GraphiteSender graphiteSender;

    /**
     * The configured {@link #METRIC_NAME_PREFIX} value.
     */
    private String metricNamePrefix;

    /**
     * Create the {@link #graphiteSender} according to configuration.
     *
     * @param context used to access properties.
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        String host = context.getProperty(HOST).evaluateAttributeExpressions().getValue();
        int port = context.getProperty(PORT).evaluateAttributeExpressions().asInteger();
        Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
        graphiteSender = createSender(host, port, charset);
        metricNamePrefix = context.getProperty(METRIC_NAME_PREFIX).evaluateAttributeExpressions().getValue();
    }

    /**
     * Close the graphite sender.
     *
     * @throws IOException if failed to close the connection.
     */
    @OnDisabled
    public void shutdown() throws IOException {
        try {
            graphiteSender.close();
        } finally {
            graphiteSender = null;
        }
    }

    /**
     * Use the {@link #graphiteSender} in order to create a reporter.
     *
     * @param metricRegistry registry with the metrics to report.
     * @return a reporter instance.
     */
    @Override
    public ScheduledReporter createReporter(MetricRegistry metricRegistry) {
        return GraphiteReporter.forRegistry(metricRegistry).prefixedWith(metricNamePrefix).build(graphiteSender);

    }

    /**
     * Create a sender.
     *
     * @param host the hostname of the server to connect to.
     * @param port the port on which the server listens.
     * @param charset the charset in which the server expects logs.
     * @return The created sender.
     */
    protected GraphiteSender createSender(String host, int port, Charset charset) {
        return new Graphite(host, port, SocketFactory.getDefault(), charset);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
