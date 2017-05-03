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

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObjectBuilder;
import javax.json.JsonValue;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;

@Tags({"status", "metrics", "history", "site", "site to site"})
@CapabilityDescription("Publishes Status events using the Site To Site protocol.  "
        + "The component type and name filter regexes form a union: only components matching both regexes will be reported.  "
        + "However, all process groups are recursively searched for matching components, regardless of whether the process group matches the component filters.")
public class SiteToSiteStatusReportingTask extends AbstractSiteToSiteReportingTask {

    static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    static final PropertyDescriptor PLATFORM = new PropertyDescriptor.Builder()
        .name("Platform")
        .description("The value to use for the platform field in each status record.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("nifi")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    static final PropertyDescriptor COMPONENT_TYPE_FILTER_REGEX = new PropertyDescriptor.Builder()
        .name("Component Type Filter Regex")
        .description("A regex specifying which component types to report.  Any component type matching this regex will be included.  "
                + "Component types are: Processor, RootProcessGroup, ProcessGroup, RemoteProcessGroup, Connection, InputPort, OutputPort")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("(Processor|ProcessGroup|RemoteProcessGroup|RootProcessGroup|Connection|InputPort|OutputPort)")
        .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
        .build();
    static final PropertyDescriptor COMPONENT_NAME_FILTER_REGEX = new PropertyDescriptor.Builder()
        .name("Component Name Filter Regex")
        .description("A regex specifying which component names to report.  Any component name matching this regex will be included.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue(".*")
        .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
        .build();

    private volatile Pattern componentTypeFilter;
    private volatile Pattern componentNameFilter;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PLATFORM);
        properties.add(COMPONENT_TYPE_FILTER_REGEX);
        properties.add(COMPONENT_NAME_FILTER_REGEX);
        return properties;
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

        componentTypeFilter = Pattern.compile(context.getProperty(COMPONENT_TYPE_FILTER_REGEX).evaluateAttributeExpressions().getValue());
        componentNameFilter = Pattern.compile(context.getProperty(COMPONENT_NAME_FILTER_REGEX).evaluateAttributeExpressions().getValue());

        final ProcessGroupStatus procGroupStatus = context.getEventAccess().getControllerStatus();
        final String rootGroupName = procGroupStatus == null ? null : procGroupStatus.getName();

        final String nifiUrl = context.getProperty(INSTANCE_URL).evaluateAttributeExpressions().getValue();
        URL url;
        try {
            url = new URL(nifiUrl);
        } catch (final MalformedURLException e1) {
            // already validated
            throw new AssertionError();
        }

        final String hostname = url.getHost();
        final String platform = context.getProperty(PLATFORM).evaluateAttributeExpressions().getValue();

        final Map<String, ?> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);

        final DateFormat df = new SimpleDateFormat(TIMESTAMP_FORMAT);
        df.setTimeZone(TimeZone.getTimeZone("Z"));

        final JsonArrayBuilder arrayBuilder = factory.createArrayBuilder();
        serializeProcessGroupStatus(arrayBuilder, factory, procGroupStatus, df, hostname, rootGroupName,
                platform, null, new Date());

        final JsonArray jsonArray = arrayBuilder.build();

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        int fromIndex = 0;
        int toIndex = Math.min(batchSize, jsonArray.size());
        List<JsonValue> jsonBatch = jsonArray.subList(fromIndex, toIndex);

        while(!jsonBatch.isEmpty()) {
            // Send the JSON document for the current batch
            try {
                long start = System.nanoTime();
                final Transaction transaction = getClient().createTransaction(TransferDirection.SEND);
                if (transaction == null) {
                    getLogger().debug("All destination nodes are penalized; will attempt to send data later");
                    return;
                }

                final Map<String, String> attributes = new HashMap<>();
                final String transactionId = UUID.randomUUID().toString();
                attributes.put("reporting.task.transaction.id", transactionId);
                attributes.put("mime.type", "application/json");

                JsonArrayBuilder jsonBatchArrayBuilder = factory.createArrayBuilder();
                for(JsonValue jsonValue : jsonBatch) {
                    jsonBatchArrayBuilder.add(jsonValue);
                }
                final JsonArray jsonBatchArray = jsonBatchArrayBuilder.build();

                final byte[] data = jsonBatchArray.toString().getBytes(StandardCharsets.UTF_8);
                transaction.send(data, attributes);
                transaction.confirm();
                transaction.complete();

                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                getLogger().info("Successfully sent {} Status Records to destination in {} ms; Transaction ID = {}",
                        new Object[]{jsonArray.size(), transferMillis, transactionId});

                fromIndex = toIndex;
                toIndex = Math.min(fromIndex + batchSize, jsonArray.size());
                jsonBatch = jsonArray.subList(fromIndex, toIndex);
            } catch (final IOException e) {
                throw new ProcessException("Failed to send Status Records to destination due to IOException:" + e.getMessage(), e);
            }
        }
    }

    /**
     * Returns true only if the component type matches the component type filter
     * and the component name matches the component name filter.
     *
     * @param componentType
     *            The component type
     * @param componentName
     *            The component name
     * @return Whether the component matches both filters
     */
    boolean componentMatchesFilters(final String componentType, final String componentName) {
        return componentTypeFilter.matcher(componentType).matches()
                && componentNameFilter.matcher(componentName).matches();
    }

    /**
     * Serialize the ProcessGroupStatus and add it to the JsonArrayBuilder.
     * @param arrayBuilder
     *            The JSON Array builder
     * @param factory
     *            The JSON Builder Factory
     * @param status
     *            The ProcessGroupStatus
     * @param df
     *            A date format
     * @param hostname
     *            The current hostname
     * @param applicationName
     *            The root process group name
     * @param platform
     *            The configured platform
     * @param parentId
     *            The parent's component id
     */
    void serializeProcessGroupStatus(final JsonArrayBuilder arrayBuilder, final JsonBuilderFactory factory,
            final ProcessGroupStatus status, final DateFormat df,
        final String hostname, final String applicationName, final String platform, final String parentId, final Date currentDate) {
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final String componentType = (parentId == null) ? "RootProcessGroup" : "ProcessGroup";
        final String componentName = status.getName();

        if (componentMatchesFilters(componentType, componentName)) {
            addCommonFields(builder, df, hostname, applicationName, platform, parentId, currentDate,
                    componentType, componentName);

            addField(builder, "componentId", status.getId());
            addField(builder, "bytesRead", status.getBytesRead());
            addField(builder, "bytesWritten", status.getBytesWritten());
            addField(builder, "bytesReceived", status.getBytesReceived());
            addField(builder, "bytesSent", status.getBytesSent());
            addField(builder, "bytesTransferred", status.getBytesTransferred());
            addField(builder, "flowFilesReceived", status.getFlowFilesReceived());
            addField(builder, "flowFilesSent", status.getFlowFilesSent());
            addField(builder, "flowFilesTransferred", status.getFlowFilesTransferred());
            addField(builder, "inputContentSize", status.getInputContentSize());
            addField(builder, "inputCount", status.getInputCount());
            addField(builder, "outputContentSize", status.getOutputContentSize());
            addField(builder, "outputCount", status.getOutputCount());
            addField(builder, "queuedContentSize", status.getQueuedContentSize());
            addField(builder, "activeThreadCount", status.getActiveThreadCount());
            addField(builder, "queuedCount", status.getQueuedCount());

            arrayBuilder.add(builder.build());
        }

        for(ProcessGroupStatus childGroupStatus : status.getProcessGroupStatus()) {
            serializeProcessGroupStatus(arrayBuilder, factory, childGroupStatus, df, hostname,
                    applicationName, platform, status.getId(), currentDate);
        }
        for(ProcessorStatus processorStatus : status.getProcessorStatus()) {
            serializeProcessorStatus(arrayBuilder, factory, processorStatus, df, hostname,
                    applicationName, platform, status.getId(), currentDate);
        }
        for(ConnectionStatus connectionStatus : status.getConnectionStatus()) {
            serializeConnectionStatus(arrayBuilder, factory, connectionStatus, df, hostname,
                    applicationName, platform, status.getId(), currentDate);
        }
        for(PortStatus portStatus : status.getInputPortStatus()) {
            serializePortStatus("InputPort", arrayBuilder, factory, portStatus, df,
                    hostname, applicationName, platform, status.getId(), currentDate);
        }
        for(PortStatus portStatus : status.getOutputPortStatus()) {
            serializePortStatus("OutputPort", arrayBuilder, factory, portStatus, df,
                    hostname, applicationName, platform, status.getId(), currentDate);
        }
        for(RemoteProcessGroupStatus remoteProcessGroupStatus : status.getRemoteProcessGroupStatus()) {
            serializeRemoteProcessGroupStatus(arrayBuilder, factory, remoteProcessGroupStatus, df, hostname,
                    applicationName, platform, status.getId(), currentDate);
        }
    }

    void serializeRemoteProcessGroupStatus(final JsonArrayBuilder arrayBuilder, final JsonBuilderFactory factory,
            final RemoteProcessGroupStatus status, final DateFormat df, final String hostname, final String applicationName,
            final String platform, final String parentId, final Date currentDate) {
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final String componentType = "RemoteProcessGroup";
        final String componentName = status.getName();

        if (componentMatchesFilters(componentType, componentName)) {
            addCommonFields(builder, df, hostname, applicationName, platform, parentId, currentDate,
                    componentType, componentName);

            addField(builder, "componentId", status.getId());
            addField(builder, "activeRemotePortCount", status.getActiveRemotePortCount());
            addField(builder, "activeThreadCount", status.getActiveThreadCount());
            addField(builder, "inactiveRemotePortCount", status.getInactiveRemotePortCount());
            addField(builder, "receivedContentSize", status.getReceivedContentSize());
            addField(builder, "receivedCount", status.getReceivedCount());
            addField(builder, "sentContentSize", status.getSentContentSize());
            addField(builder, "sentCount", status.getSentCount());
            addField(builder, "averageLineageDuration", status.getAverageLineageDuration());

            arrayBuilder.add(builder.build());
        }
    }

    void serializePortStatus(final String componentType, final JsonArrayBuilder arrayBuilder, final JsonBuilderFactory factory, final PortStatus status,
            final DateFormat df, final String hostname, final String applicationName, final String platform, final String parentId, final Date currentDate) {
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final String componentName = status.getName();

        if (componentMatchesFilters(componentType, componentName)) {
            addCommonFields(builder, df, hostname, applicationName, platform, parentId, currentDate,
                    componentType, componentName);

            addField(builder, "componentId", status.getId());
            addField(builder, "activeThreadCount", status.getActiveThreadCount());
            addField(builder, "bytesReceived", status.getBytesReceived());
            addField(builder, "bytesSent", status.getBytesSent());
            addField(builder, "flowFilesReceived", status.getFlowFilesReceived());
            addField(builder, "flowFilesSent", status.getFlowFilesSent());
            addField(builder, "inputBytes", status.getInputBytes());
            addField(builder, "inputCount", status.getInputCount());
            addField(builder, "outputBytes", status.getOutputBytes());
            addField(builder, "outputCount", status.getOutputCount());

            arrayBuilder.add(builder.build());
        }
    }

    void serializeConnectionStatus(final JsonArrayBuilder arrayBuilder, final JsonBuilderFactory factory, final ConnectionStatus status, final DateFormat df,
            final String hostname, final String applicationName, final String platform, final String parentId, final Date currentDate) {
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final String componentType = "Connection";
        final String componentName = status.getName();

        if (componentMatchesFilters(componentType, componentName)) {
            addCommonFields(builder, df, hostname, applicationName, platform, parentId, currentDate,
                    componentType, componentName);

            addField(builder, "componentId", status.getId());
            addField(builder, "maxQueuedBytes", status.getMaxQueuedBytes());
            addField(builder, "maxQueuedCount", status.getMaxQueuedCount());
            addField(builder, "queuedBytes", status.getQueuedBytes());
            addField(builder, "queuedCount", status.getQueuedCount());
            addField(builder, "inputBytes", status.getInputBytes());
            addField(builder, "inputCount", status.getInputCount());
            addField(builder, "outputBytes", status.getOutputBytes());
            addField(builder, "outputCount", status.getOutputCount());
            addField(builder, "backPressureBytesThreshold", status.getBackPressureBytesThreshold());
            addField(builder, "backPressureObjectThreshold", status.getBackPressureObjectThreshold());
            addField(builder, "isBackPressureEnabled", Boolean.toString((status.getBackPressureObjectThreshold() > 0 && status.getBackPressureObjectThreshold() <= status.getQueuedCount())
                    || (status.getBackPressureBytesThreshold() > 0 && status.getBackPressureBytesThreshold() <= status.getMaxQueuedBytes())));

            arrayBuilder.add(builder.build());
        }
    }

    void serializeProcessorStatus(final JsonArrayBuilder arrayBuilder, final JsonBuilderFactory factory, final ProcessorStatus status, final DateFormat df,
            final String hostname, final String applicationName, final String platform, final String parentId, final Date currentDate) {
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final String componentType = "Processor";
        final String componentName = status.getName();

        if (componentMatchesFilters(componentType, componentName)) {
            addCommonFields(builder, df, hostname, applicationName, platform, parentId, currentDate, componentType, componentName);

            addField(builder, "componentId", status.getId());
            addField(builder, "processorType", status.getType());
            addField(builder, "averageLineageDurationMS", status.getAverageLineageDuration());
            addField(builder, "bytesRead", status.getBytesRead());
            addField(builder, "bytesWritten", status.getBytesWritten());
            addField(builder, "bytesReceived", status.getBytesReceived());
            addField(builder, "bytesSent", status.getBytesSent());
            addField(builder, "flowFilesRemoved", status.getFlowFilesRemoved());
            addField(builder, "flowFilesReceived", status.getFlowFilesReceived());
            addField(builder, "flowFilesSent", status.getFlowFilesSent());
            addField(builder, "inputCount", status.getInputCount());
            addField(builder, "inputBytes", status.getInputBytes());
            addField(builder, "outputCount", status.getOutputCount());
            addField(builder, "outputBytes", status.getOutputBytes());
            addField(builder, "activeThreadCount", status.getActiveThreadCount());
            addField(builder, "invocations", status.getInvocations());
            addField(builder, "processingNanos", status.getProcessingNanos());

            arrayBuilder.add(builder.build());
        }
    }

    private static void addCommonFields(final JsonObjectBuilder builder, final DateFormat df, final String hostname,
            final String applicationName, final String platform, final String parentId, final Date currentDate,
            final String componentType, final String componentName) {
        addField(builder, "statusId", UUID.randomUUID().toString());
        addField(builder, "timestampMillis", currentDate.getTime());
        addField(builder, "timestamp", df.format(currentDate));
        addField(builder, "actorHostname", hostname);
        addField(builder, "componentType", componentType);
        addField(builder, "componentName", componentName);
        addField(builder, "parentId", parentId);
        addField(builder, "platform", platform);
        addField(builder, "application", applicationName);
    }

    private static void addField(final JsonObjectBuilder builder, final String key, final Long value) {
        if (value != null) {
            builder.add(key, value.longValue());
        }
    }

    private static void addField(final JsonObjectBuilder builder, final String key, final Integer value) {
        if (value != null) {
            builder.add(key, value.intValue());
        }
    }

    private static void addField(final JsonObjectBuilder builder, final String key, final String value) {
        if (value == null) {
            return;
        }

        builder.add(key, value);
    }
}
