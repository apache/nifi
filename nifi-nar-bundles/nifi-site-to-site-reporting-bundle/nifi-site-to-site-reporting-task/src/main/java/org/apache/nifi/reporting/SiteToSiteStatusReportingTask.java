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
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
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

import org.apache.avro.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.reporting.s2s.SiteToSiteUtils;

@Tags({"status", "metrics", "history", "site", "site to site"})
@CapabilityDescription("Publishes Status events using the Site To Site protocol.  "
        + "The component type and name filter regexes form a union: only components matching both regexes will be reported.  "
        + "However, all process groups are recursively searched for matching components, regardless of whether the process group matches the component filters.")
public class SiteToSiteStatusReportingTask extends AbstractSiteToSiteReportingTask {

    static final PropertyDescriptor PLATFORM = new PropertyDescriptor.Builder()
        .name("Platform")
        .description("The value to use for the platform field in each status record.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .defaultValue("nifi")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor COMPONENT_TYPE_FILTER_REGEX = new PropertyDescriptor.Builder()
        .name("Component Type Filter Regex")
        .description("A regex specifying which component types to report.  Any component type matching this regex will be included.  "
                + "Component types are: Processor, RootProcessGroup, ProcessGroup, RemoteProcessGroup, Connection, InputPort, OutputPort")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .defaultValue("(Processor|ProcessGroup|RemoteProcessGroup|RootProcessGroup|Connection|InputPort|OutputPort)")
        .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
        .build();

    static final PropertyDescriptor COMPONENT_NAME_FILTER_REGEX = new PropertyDescriptor.Builder()
        .name("Component Name Filter Regex")
        .description("A regex specifying which component names to report.  Any component name matching this regex will be included.")
        .required(true)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .defaultValue(".*")
        .addValidator(StandardValidators.createRegexValidator(0, Integer.MAX_VALUE, true))
        .build();

    private volatile Pattern componentTypeFilter;
    private volatile Pattern componentNameFilter;
    private volatile Map<String,String> processGroupIDToPath;

    public SiteToSiteStatusReportingTask() throws IOException {
        final InputStream schema = getClass().getClassLoader().getResourceAsStream("schema-status.avsc");
        recordSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schema));
    }

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

        // initialize the map
        processGroupIDToPath = new HashMap<String,String>();

        final ProcessGroupStatus procGroupStatus = context.getEventAccess().getControllerStatus();
        final String rootGroupName = procGroupStatus == null ? null : procGroupStatus.getName();

        final String nifiUrl = context.getProperty(SiteToSiteUtils.INSTANCE_URL).evaluateAttributeExpressions().getValue();
        URL url;
        try {
            url = new URL(nifiUrl);
        } catch (final MalformedURLException e1) {
            // already validated
            throw new AssertionError();
        }

        final String hostname = url.getHost();
        final String platform = context.getProperty(PLATFORM).evaluateAttributeExpressions().getValue();
        final Boolean allowNullValues = context.getProperty(ALLOW_NULL_VALUES).asBoolean();

        final Map<String, ?> config = Collections.emptyMap();
        final JsonBuilderFactory factory = Json.createBuilderFactory(config);

        final DateFormat df = new SimpleDateFormat(TIMESTAMP_FORMAT);
        df.setTimeZone(TimeZone.getTimeZone("Z"));

        final JsonArrayBuilder arrayBuilder = factory.createArrayBuilder();
        serializeProcessGroupStatus(arrayBuilder, factory, procGroupStatus, df,
                hostname, rootGroupName, platform, null, new Date(), allowNullValues);

        final JsonArray jsonArray = arrayBuilder.build();

        final int batchSize = context.getProperty(SiteToSiteUtils.BATCH_SIZE).asInteger();
        int fromIndex = 0;
        int toIndex = Math.min(batchSize, jsonArray.size());
        List<JsonValue> jsonBatch = jsonArray.subList(fromIndex, toIndex);

        while(!jsonBatch.isEmpty()) {
            // Send the JSON document for the current batch
            Transaction transaction = null;
            try {
                // Lazily create SiteToSiteClient to provide a StateManager
                setup(context);

                long start = System.nanoTime();
                transaction = getClient().createTransaction(TransferDirection.SEND);
                if (transaction == null) {
                    getLogger().debug("All destination nodes are penalized; will attempt to send data later");
                    return;
                }

                final Map<String, String> attributes = new HashMap<>();
                final String transactionId = UUID.randomUUID().toString();
                attributes.put("reporting.task.transaction.id", transactionId);
                attributes.put("reporting.task.name", getName());
                attributes.put("reporting.task.uuid", getIdentifier());
                attributes.put("reporting.task.type", this.getClass().getSimpleName());
                attributes.put("mime.type", "application/json");

                JsonArrayBuilder jsonBatchArrayBuilder = factory.createArrayBuilder();
                for(JsonValue jsonValue : jsonBatch) {
                    jsonBatchArrayBuilder.add(jsonValue);
                }

                final JsonArray jsonBatchArray = jsonBatchArrayBuilder.build();
                sendData(context, transaction, attributes, jsonBatchArray);
                transaction.confirm();
                transaction.complete();

                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                getLogger().info("Successfully sent {} Status Records to destination in {} ms; Transaction ID = {}",
                        new Object[]{jsonArray.size(), transferMillis, transactionId});

                fromIndex = toIndex;
                toIndex = Math.min(fromIndex + batchSize, jsonArray.size());
                jsonBatch = jsonArray.subList(fromIndex, toIndex);
            } catch (final Exception e) {
                if (transaction != null) {
                    transaction.error();
                }
                if (e instanceof ProcessException) {
                    throw (ProcessException) e;
                } else {
                    throw new ProcessException("Failed to send Status Records to destination due to IOException:" + e.getMessage(), e);
                }
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
    private boolean componentMatchesFilters(final String componentType, final String componentName) {
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
     * @param parent
     *            The parent's process group status object
     * @param currentDate
     *            The current date
     * @param allowNullValues
     *            Allow null values
     */
    private void serializeProcessGroupStatus(final JsonArrayBuilder arrayBuilder, final JsonBuilderFactory factory,
            final ProcessGroupStatus status, final DateFormat df, final String hostname, final String applicationName,
            final String platform, final ProcessGroupStatus parent, final Date currentDate, Boolean allowNullValues) {
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final String componentType = parent == null ? "RootProcessGroup" : "ProcessGroup";
        final String componentName = status.getName();

        if(parent == null) {
            processGroupIDToPath.put(status.getId(), "NiFi Flow");
        }

        if (componentMatchesFilters(componentType, componentName)) {
            addCommonFields(builder, df, hostname, applicationName, platform, parent, currentDate,
                    componentType, componentName, allowNullValues);

            addField(builder, "componentId", status.getId(), allowNullValues);
            addField(builder, "bytesRead", status.getBytesRead(), allowNullValues);
            addField(builder, "bytesWritten", status.getBytesWritten(), allowNullValues);
            addField(builder, "bytesReceived", status.getBytesReceived(), allowNullValues);
            addField(builder, "bytesSent", status.getBytesSent(), allowNullValues);
            addField(builder, "bytesTransferred", status.getBytesTransferred(), allowNullValues);
            addField(builder, "flowFilesReceived", status.getFlowFilesReceived(), allowNullValues);
            addField(builder, "flowFilesSent", status.getFlowFilesSent(), allowNullValues);
            addField(builder, "flowFilesTransferred", status.getFlowFilesTransferred(), allowNullValues);
            addField(builder, "inputContentSize", status.getInputContentSize(), allowNullValues);
            addField(builder, "inputCount", status.getInputCount(), allowNullValues);
            addField(builder, "outputContentSize", status.getOutputContentSize(), allowNullValues);
            addField(builder, "outputCount", status.getOutputCount(), allowNullValues);
            addField(builder, "queuedContentSize", status.getQueuedContentSize(), allowNullValues);
            addField(builder, "activeThreadCount", status.getActiveThreadCount(), allowNullValues);
            addField(builder, "terminatedThreadCount", status.getTerminatedThreadCount(), allowNullValues);
            addField(builder, "queuedCount", status.getQueuedCount(), allowNullValues);
            addField(builder, "versionedFlowState", status.getVersionedFlowState() == null ? null : status.getVersionedFlowState().name(), allowNullValues);

            arrayBuilder.add(builder.build());
        }

        for(ProcessGroupStatus childGroupStatus : status.getProcessGroupStatus()) {

            processGroupIDToPath.put(childGroupStatus.getId(), processGroupIDToPath.get(status.getId()) + " / " + childGroupStatus.getName());

            serializeProcessGroupStatus(arrayBuilder, factory, childGroupStatus, df, hostname,
                    applicationName, platform, status, currentDate, allowNullValues);
        }
        for(ProcessorStatus processorStatus : status.getProcessorStatus()) {
            serializeProcessorStatus(arrayBuilder, factory, processorStatus, df, hostname,
                    applicationName, platform, status, currentDate, allowNullValues);
        }
        for(ConnectionStatus connectionStatus : status.getConnectionStatus()) {
            serializeConnectionStatus(arrayBuilder, factory, connectionStatus, df, hostname,
                    applicationName, platform, status, currentDate, allowNullValues);
        }
        for(PortStatus portStatus : status.getInputPortStatus()) {
            serializePortStatus("InputPort", arrayBuilder, factory, portStatus, df,
                    hostname, applicationName, platform, status, currentDate, allowNullValues);
        }
        for(PortStatus portStatus : status.getOutputPortStatus()) {
            serializePortStatus("OutputPort", arrayBuilder, factory, portStatus, df,
                    hostname, applicationName, platform, status, currentDate, allowNullValues);
        }
        for(RemoteProcessGroupStatus remoteProcessGroupStatus : status.getRemoteProcessGroupStatus()) {
            serializeRemoteProcessGroupStatus(arrayBuilder, factory, remoteProcessGroupStatus, df, hostname,
                    applicationName, platform, status, currentDate, allowNullValues);
        }
    }

    private void serializeRemoteProcessGroupStatus(final JsonArrayBuilder arrayBuilder, final JsonBuilderFactory factory,
            final RemoteProcessGroupStatus status, final DateFormat df, final String hostname, final String applicationName,
            final String platform, final ProcessGroupStatus parent, final Date currentDate, final Boolean allowNullValues) {
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final String componentType = "RemoteProcessGroup";
        final String componentName = status.getName();

        if (componentMatchesFilters(componentType, componentName)) {
            addCommonFields(builder, df, hostname, applicationName, platform, parent, currentDate,
                    componentType, componentName, allowNullValues);

            addField(builder, "componentId", status.getId(), allowNullValues);
            addField(builder, "activeRemotePortCount", status.getActiveRemotePortCount(), allowNullValues);
            addField(builder, "activeThreadCount", status.getActiveThreadCount(), allowNullValues);
            addField(builder, "inactiveRemotePortCount", status.getInactiveRemotePortCount(), allowNullValues);
            addField(builder, "receivedContentSize", status.getReceivedContentSize(), allowNullValues);
            addField(builder, "receivedCount", status.getReceivedCount(), allowNullValues);
            addField(builder, "sentContentSize", status.getSentContentSize(), allowNullValues);
            addField(builder, "sentCount", status.getSentCount(), allowNullValues);
            addField(builder, "averageLineageDuration", status.getAverageLineageDuration(), allowNullValues);
            addField(builder, "transmissionStatus", status.getTransmissionStatus() == null ? null : status.getTransmissionStatus().name(), allowNullValues);
            addField(builder, "targetURI", status.getTargetUri(), allowNullValues);

            arrayBuilder.add(builder.build());
        }
    }

    private void serializePortStatus(final String componentType, final JsonArrayBuilder arrayBuilder, final JsonBuilderFactory factory, final PortStatus status,
            final DateFormat df, final String hostname, final String applicationName, final String platform, final ProcessGroupStatus parent, final Date currentDate, final Boolean allowNullValues) {
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final String componentName = status.getName();

        if (componentMatchesFilters(componentType, componentName)) {
            addCommonFields(builder, df, hostname, applicationName, platform, parent, currentDate,
                    componentType, componentName, allowNullValues);

            addField(builder, "componentId", status.getId(), allowNullValues);
            addField(builder, "activeThreadCount", status.getActiveThreadCount(), allowNullValues);
            addField(builder, "bytesReceived", status.getBytesReceived(), allowNullValues);
            addField(builder, "bytesSent", status.getBytesSent(), allowNullValues);
            addField(builder, "flowFilesReceived", status.getFlowFilesReceived(), allowNullValues);
            addField(builder, "flowFilesSent", status.getFlowFilesSent(), allowNullValues);
            addField(builder, "inputBytes", status.getInputBytes(), allowNullValues);
            addField(builder, "inputCount", status.getInputCount(), allowNullValues);
            addField(builder, "outputBytes", status.getOutputBytes(), allowNullValues);
            addField(builder, "outputCount", status.getOutputCount(), allowNullValues);
            addField(builder, "runStatus", status.getRunStatus() == null ? null : status.getRunStatus().name(), allowNullValues);
            addField(builder, "transmitting", status.isTransmitting(), allowNullValues);

            arrayBuilder.add(builder.build());
        }
    }

    private void serializeConnectionStatus(final JsonArrayBuilder arrayBuilder, final JsonBuilderFactory factory, final ConnectionStatus status, final DateFormat df,
            final String hostname, final String applicationName, final String platform, final ProcessGroupStatus parent, final Date currentDate, final Boolean allowNullValues) {
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final String componentType = "Connection";
        final String componentName = status.getName();

        if (componentMatchesFilters(componentType, componentName)) {
            addCommonFields(builder, df, hostname, applicationName, platform, parent, currentDate,
                    componentType, componentName, allowNullValues);

            addField(builder, "componentId", status.getId(), allowNullValues);
            addField(builder, "sourceId", status.getSourceId(), allowNullValues);
            addField(builder, "sourceName", status.getSourceName(), allowNullValues);
            addField(builder, "destinationId", status.getDestinationId(), allowNullValues);
            addField(builder, "destinationName", status.getDestinationName(), allowNullValues);
            addField(builder, "maxQueuedBytes", status.getMaxQueuedBytes(), allowNullValues);
            addField(builder, "maxQueuedCount", status.getMaxQueuedCount(), allowNullValues);
            addField(builder, "queuedBytes", status.getQueuedBytes(), allowNullValues);
            addField(builder, "queuedCount", status.getQueuedCount(), allowNullValues);
            addField(builder, "inputBytes", status.getInputBytes(), allowNullValues);
            addField(builder, "inputCount", status.getInputCount(), allowNullValues);
            addField(builder, "outputBytes", status.getOutputBytes(), allowNullValues);
            addField(builder, "outputCount", status.getOutputCount(), allowNullValues);
            addField(builder, "backPressureBytesThreshold", status.getBackPressureBytesThreshold(), allowNullValues);
            addField(builder, "backPressureObjectThreshold", status.getBackPressureObjectThreshold(), allowNullValues);
            addField(builder, "backPressureDataSizeThreshold", status.getBackPressureDataSizeThreshold(), allowNullValues);
            addField(builder, "isBackPressureEnabled", Boolean.toString((status.getBackPressureObjectThreshold() > 0 && status.getBackPressureObjectThreshold() <= status.getQueuedCount())
                    || (status.getBackPressureBytesThreshold() > 0 && status.getBackPressureBytesThreshold() <= status.getQueuedBytes())), allowNullValues);

            arrayBuilder.add(builder.build());
        }
    }

    private void serializeProcessorStatus(final JsonArrayBuilder arrayBuilder, final JsonBuilderFactory factory, final ProcessorStatus status, final DateFormat df,
            final String hostname, final String applicationName, final String platform, final ProcessGroupStatus parent, final Date currentDate, final Boolean allowNullValues) {
        final JsonObjectBuilder builder = factory.createObjectBuilder();
        final String componentType = "Processor";
        final String componentName = status.getName();

        if (componentMatchesFilters(componentType, componentName)) {
            addCommonFields(builder, df, hostname, applicationName, platform, parent, currentDate, componentType, componentName, allowNullValues);

            addField(builder, "componentId", status.getId(), allowNullValues);
            addField(builder, "processorType", status.getType(), allowNullValues);
            addField(builder, "averageLineageDurationMS", status.getAverageLineageDuration(), allowNullValues);
            addField(builder, "bytesRead", status.getBytesRead(), allowNullValues);
            addField(builder, "bytesWritten", status.getBytesWritten(), allowNullValues);
            addField(builder, "bytesReceived", status.getBytesReceived(), allowNullValues);
            addField(builder, "bytesSent", status.getBytesSent(), allowNullValues);
            addField(builder, "flowFilesRemoved", status.getFlowFilesRemoved(), allowNullValues);
            addField(builder, "flowFilesReceived", status.getFlowFilesReceived(), allowNullValues);
            addField(builder, "flowFilesSent", status.getFlowFilesSent(), allowNullValues);
            addField(builder, "inputCount", status.getInputCount(), allowNullValues);
            addField(builder, "inputBytes", status.getInputBytes(), allowNullValues);
            addField(builder, "outputCount", status.getOutputCount(), allowNullValues);
            addField(builder, "outputBytes", status.getOutputBytes(), allowNullValues);
            addField(builder, "activeThreadCount", status.getActiveThreadCount(), allowNullValues);
            addField(builder, "terminatedThreadCount", status.getTerminatedThreadCount(), allowNullValues);
            addField(builder, "invocations", status.getInvocations(), allowNullValues);
            addField(builder, "processingNanos", status.getProcessingNanos(), allowNullValues);
            addField(builder, "runStatus", status.getRunStatus() == null ? null : status.getRunStatus().name(), allowNullValues);
            addField(builder, "executionNode", status.getExecutionNode() == null ? null : status.getExecutionNode().name(), allowNullValues);
            addField(builder, factory, "counters", status.getCounters(), allowNullValues);

            arrayBuilder.add(builder.build());
        }
    }

    private void addCommonFields(final JsonObjectBuilder builder, final DateFormat df, final String hostname,
            final String applicationName, final String platform, final ProcessGroupStatus parent, final Date currentDate,
            final String componentType, final String componentName, Boolean allowNullValues) {
        addField(builder, "statusId", UUID.randomUUID().toString(), allowNullValues);
        addField(builder, "timestampMillis", currentDate.getTime(), allowNullValues);
        addField(builder, "timestamp", df.format(currentDate), allowNullValues);
        addField(builder, "actorHostname", hostname, allowNullValues);
        addField(builder, "componentType", componentType, allowNullValues);
        addField(builder, "componentName", componentName, allowNullValues);
        addField(builder, "parentId", parent == null ? null : parent.getId(), allowNullValues);
        addField(builder, "parentName", parent == null ? null : parent.getName(), allowNullValues);
        addField(builder, "parentPath", parent == null ? null : processGroupIDToPath.get(parent.getId()), allowNullValues);
        addField(builder, "platform", platform, allowNullValues);
        addField(builder, "application", applicationName, allowNullValues);
    }

    private static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key, final Map<String, Long> values, final Boolean allowNullValues) {

        if (values != null) {

            final JsonObjectBuilder mapBuilder = factory.createObjectBuilder();
            for (final Map.Entry<String, Long> entry : values.entrySet()) {

                if (entry.getKey() == null ) {
                    continue;
                }else if(entry.getValue() == null ){
                    if(allowNullValues)
                        mapBuilder.add(entry.getKey(),JsonValue.NULL);
                }else{
                    mapBuilder.add(entry.getKey(), entry.getValue());
                }
            }

            builder.add(key, mapBuilder);

        }else if(allowNullValues){
            builder.add(key,JsonValue.NULL);
        }
    }
}
