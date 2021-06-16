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
import io.prometheus.client.Gauge;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.prometheus.util.PrometheusMetricsUtil;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.eclipse.jetty.server.Server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@Tags({"record", "send", "write", "prometheus"})
@CapabilityDescription("Specifies a Record Sink Service that exposes data points to a Prometheus scraping service. Numeric fields are exposed as Gauges, String fields are the "
        + "label values for the gauges, and all other fields are ignored.")

public class PrometheusRecordSink extends AbstractControllerService implements RecordSinkService {

    private volatile PrometheusServer prometheusServer;
    private volatile RecordSchema recordSchema;
    private volatile String[] labelNames;
    private volatile Map<String, Gauge> gauges;
    private static final CollectorRegistry RECORD_REGISTRY = new CollectorRegistry();

    public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
            .name("prometheus-reporting-task-ssl-context")
            .displayName("SSL Context Service")
            .description("The SSL Context Service to use in order to secure the server. If specified, the server will"
                    + "accept only HTTPS requests; otherwise, the server will accept only HTTP requests")
            .required(false)
            .identifiesControllerService(RestrictedSSLContextService.class)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PrometheusMetricsUtil.METRICS_ENDPOINT_PORT);
        props.add(PrometheusMetricsUtil.INSTANCE_ID);
        props.add(SSL_CONTEXT);
        props.add(PrometheusMetricsUtil.CLIENT_AUTH);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onScheduled(final ConfigurationContext context) {
        RECORD_REGISTRY.clear();
        SSLContextService sslContextService = context.getProperty(SSL_CONTEXT).asControllerService(SSLContextService.class);
        final String metricsEndpointPort = context.getProperty(PrometheusMetricsUtil.METRICS_ENDPOINT_PORT).evaluateAttributeExpressions().getValue();

        try {
            List<Function<ReportingContext, CollectorRegistry>> metricsCollectors = new ArrayList<>();
            if (sslContextService == null) {
                prometheusServer = new PrometheusServer(new InetSocketAddress(Integer.parseInt(metricsEndpointPort)), getLogger());
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
                prometheusServer = new PrometheusServer(Integer.parseInt(metricsEndpointPort), sslContextService, getLogger(), need, want);
            }
            Function<ReportingContext, CollectorRegistry> nifiMetrics = (reportingContext) -> RECORD_REGISTRY;
            metricsCollectors.add(nifiMetrics);

            prometheusServer.setMetricsCollectors(metricsCollectors);
            getLogger().info("Started Jetty server");
        } catch (Exception e) {
            // Don't allow this to finish successfully, onTrigger should not be called if the Jetty server wasn't started
            throw new ProcessException("Failed to start Jetty server", e);
        }
    }

    @Override
    public WriteResult sendData(RecordSet recordSet, Map<String, String> attributes, boolean sendZeroResults) throws IOException {
        WriteResult writeResult = null;
        if (recordSchema == null) {
            // The first time through, create the registry, then create the Gauges and register them
            recordSchema = recordSet.getSchema();
            RECORD_REGISTRY.clear();

            // String fields are labels, collect them first
            labelNames = recordSchema.getFields().stream().filter(
                    (f) -> isLabel(f.getDataType().getFieldType())).map(RecordField::getFieldName).toArray(String[]::new);

            gauges = new HashMap<>();
            recordSchema.getFields().stream().filter((field) -> isNumeric(field.getDataType().getFieldType())).forEach(
                    // Create, register, and add gauge to the list
                    (field) -> gauges.put(field.getFieldName(), Gauge.build()
                            .name(field.getFieldName())
                            .help("Metric for " + field.getFieldName())
                            .labelNames(labelNames)
                            .register(RECORD_REGISTRY))
            );
        }
        int recordCount = 0;
        Record r;
        while ((r = recordSet.next()) != null) {
            final Record record = r;
            // Get label values, set empty strings for null values
            String[] labelValues = Arrays.stream(labelNames).map((labelName) -> {
                String value = record.getAsString(labelName);
                return (value != null) ? value : "";
            }).toArray(String[]::new);

            // Get value for each gauge and update the data point

            gauges.forEach((name, gauge) -> {
                Optional<DataType> dataType = record.getSchema().getDataType(name);
                if (dataType.isPresent()) {
                    RecordFieldType recordFieldType = dataType.get().getFieldType();
                    // Change boolean fields to doubles
                    final double value;
                    if (RecordFieldType.BOOLEAN.equals(recordFieldType)) {
                        value = record.getAsBoolean(name) ? 1.0 : 0.0;
                    } else {
                        value = record.getAsDouble(name);
                    }
                    gauge.labels(labelValues).set(value);
                }
            });
            recordCount++;
        }
        attributes.put("record.count", Integer.toString(recordCount));
        writeResult = WriteResult.of(recordCount, attributes);
        return writeResult;
    }

    @OnDisabled
    public void onStopped() throws Exception {
        if (prometheusServer != null) {
            Server server = prometheusServer.getServer();
            if (server != null) {
                server.stop();
            }
        }
        recordSchema = null;
    }

    @OnShutdown
    public void onShutDown() throws Exception {
        if (prometheusServer != null) {
            Server server = prometheusServer.getServer();
            if (server != null) {
                server.stop();
            }
        }
        recordSchema = null;
    }

    @Override
    public void reset() {
        // Reset the schema in order to support different RecordSet schemas
        recordSchema = null;
    }

    private boolean isNumeric(RecordFieldType dataType) {
        // Numeric fields are metrics
        return RecordFieldType.INT.equals(dataType)
                || RecordFieldType.SHORT.equals(dataType)
                || RecordFieldType.LONG.equals(dataType)
                || RecordFieldType.BIGINT.equals(dataType)
                || RecordFieldType.FLOAT.equals(dataType)
                || RecordFieldType.DOUBLE.equals(dataType)
                || RecordFieldType.DECIMAL.equals(dataType)
                || RecordFieldType.BOOLEAN.equals(dataType);

    }

    private boolean isLabel(RecordFieldType dataType) {
        return RecordFieldType.STRING.equals(dataType)
                || RecordFieldType.CHAR.equals(dataType);
    }
}
