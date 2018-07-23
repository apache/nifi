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

package org.apache.nifi.reporting;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;

import org.apache.avro.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.reporting.util.metrics.MetricNames;
import org.apache.nifi.reporting.util.metrics.MetricsService;
import org.apache.nifi.reporting.util.metrics.api.MetricsBuilder;

import com.yammer.metrics.core.VirtualMachineMetrics;

@Tags({"status", "metrics", "site", "site to site"})
@CapabilityDescription("Publishes same metrics as the Ambari Reporting task using the Site To Site protocol.")
public class SiteToSiteMetricsReportingTask extends AbstractSiteToSiteReportingTask {

    static final AllowableValue AMBARI_FORMAT = new AllowableValue("ambari-format", "Ambari Format", "Metrics will be formatted"
            + " according to the Ambari Metrics API. See Additional Details in Usage documentation.");
    static final AllowableValue RECORD_FORMAT = new AllowableValue("record-format", "Record Format", "Metrics will be formatted"
            + " using the Record Writer property of this reporting task. See Additional Details in Usage documentation to"
            + " have the description of the default schema.");

    static final PropertyDescriptor APPLICATION_ID = new PropertyDescriptor.Builder()
            .name("s2s-metrics-application-id")
            .displayName("Application ID")
            .description("The Application ID to be included in the metrics")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
            .name("s2s-metrics-hostname")
            .displayName("Hostname")
            .description("The Hostname of this NiFi instance to be included in the metrics")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("${hostname(true)}")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FORMAT = new PropertyDescriptor.Builder()
            .name("s2s-metrics-format")
            .displayName("Output Format")
            .description("The output format that will be used for the metrics. If " + RECORD_FORMAT.getDisplayName() + " is selected, "
                    + "a Record Writer must be provided. If " + AMBARI_FORMAT.getDisplayName() + " is selected, the Record Writer property "
                    + "should be empty.")
            .required(true)
            .allowableValues(AMBARI_FORMAT, RECORD_FORMAT)
            .defaultValue(AMBARI_FORMAT.getValue())
            .addValidator(Validator.VALID)
            .build();

    private final MetricsService metricsService = new MetricsService();

    public SiteToSiteMetricsReportingTask() throws IOException {
        final InputStream schema = getClass().getClassLoader().getResourceAsStream("schema-metrics.avsc");
        recordSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schema));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(HOSTNAME);
        properties.add(APPLICATION_ID);
        properties.add(FORMAT);
        properties.remove(BATCH_SIZE);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        final boolean isWriterSet = validationContext.getProperty(RECORD_WRITER).isSet();
        if (validationContext.getProperty(FORMAT).getValue().equals(RECORD_FORMAT.getValue()) && !isWriterSet) {
            problems.add(new ValidationResult.Builder()
                    .input("Record Writer")
                    .valid(false)
                    .explanation("If using " + RECORD_FORMAT.getDisplayName() + ", a record writer needs to be set.")
                    .build());
        }
        if (validationContext.getProperty(FORMAT).getValue().equals(AMBARI_FORMAT.getValue()) && isWriterSet) {
            problems.add(new ValidationResult.Builder()
                    .input("Record Writer")
                    .valid(false)
                    .explanation("If using " + AMBARI_FORMAT.getDisplayName() + ", no record writer should be set.")
                    .build());
        }

        return problems;
    }

    @Override
    public void onTrigger(final ReportingContext context) {
        final boolean isClustered = context.isClustered();
        final String nodeId = context.getClusterNodeIdentifier();
        if (nodeId == null && isClustered) {
            getLogger().debug("This instance of NiFi is configured for clustering, but the Cluster Node Identifier is not yet available. "
                    + "Will wait for Node Identifier to be established.");
            return;
        }

        final VirtualMachineMetrics virtualMachineMetrics = VirtualMachineMetrics.getInstance();
        final Map<String, ?> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);

        final String applicationId = context.getProperty(APPLICATION_ID).evaluateAttributeExpressions().getValue();
        final String hostname = context.getProperty(HOSTNAME).evaluateAttributeExpressions().getValue();
        final ProcessGroupStatus status = context.getEventAccess().getControllerStatus();

        if(status != null) {
            final Map<String,String> statusMetrics = metricsService.getMetrics(status, false);
            final Map<String,String> jvmMetrics = metricsService.getMetrics(virtualMachineMetrics);

            final MetricsBuilder metricsBuilder = new MetricsBuilder(factory);
            final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            final double systemLoad = os.getSystemLoadAverage();

            byte[] data;
            final Map<String, String> attributes = new HashMap<>();

            if(context.getProperty(FORMAT).getValue().equals(AMBARI_FORMAT.getValue())) {
                final JsonObject metricsObject = metricsBuilder
                        .applicationId(applicationId)
                        .instanceId(status.getId())
                        .hostname(hostname)
                        .timestamp(System.currentTimeMillis())
                        .addAllMetrics(statusMetrics)
                        .addAllMetrics(jvmMetrics)
                        .metric(MetricNames.CORES, String.valueOf(os.getAvailableProcessors()))
                        .metric(MetricNames.LOAD1MN, String.valueOf(systemLoad >= 0 ? systemLoad : -1))
                        .build();

                data = metricsObject.toString().getBytes(StandardCharsets.UTF_8);
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
            } else {
                final JsonObject metricsObject = metricsService.getMetrics(factory, status, virtualMachineMetrics, applicationId, status.getId(),
                        hostname, System.currentTimeMillis(), os.getAvailableProcessors(), systemLoad >= 0 ? systemLoad : -1);
                data = getData(context, new ByteArrayInputStream(metricsObject.toString().getBytes(StandardCharsets.UTF_8)), attributes);
            }

            try {
                long start = System.nanoTime();
                final Transaction transaction = getClient().createTransaction(TransferDirection.SEND);
                if (transaction == null) {
                    getLogger().debug("All destination nodes are penalized; will attempt to send data later");
                    return;
                }

                final String transactionId = UUID.randomUUID().toString();
                attributes.put("reporting.task.transaction.id", transactionId);
                attributes.put("reporting.task.name", getName());
                attributes.put("reporting.task.uuid", getIdentifier());
                attributes.put("reporting.task.type", this.getClass().getSimpleName());

                transaction.send(data, attributes);
                transaction.confirm();
                transaction.complete();

                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                getLogger().info("Successfully sent metrics to destination in {}ms; Transaction ID = {}", new Object[]{transferMillis, transactionId});
            } catch (final Exception e) {
                throw new ProcessException("Failed to send metrics to destination due to:" + e.getMessage(), e);
            }

        } else {
            getLogger().error("No process group status to retrieve metrics");
        }
    }

}
