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

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.reporting.util.provenance.ProvenanceEventConsumer;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Tags({"provenance", "lineage", "tracking", "site", "site to site"})
@CapabilityDescription("Publishes Provenance events using the Site To Site protocol.")
@Stateful(scopes = Scope.LOCAL, description = "Stores the Reporting Task's last event Id so that on restart the task knows where it left off.")
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.EXPORT_NIFI_DETAILS,
                        explanation = "Provides operator the ability to send sensitive details contained in Provenance events to any external system.")
        }
)
public class SiteToSiteProvenanceReportingTask extends AbstractSiteToSiteReportingTask {

    static final AllowableValue BEGINNING_OF_STREAM = new AllowableValue("beginning-of-stream", "Beginning of Stream",
            "Start reading provenance Events from the beginning of the stream (the oldest event first)");
    static final AllowableValue END_OF_STREAM = new AllowableValue("end-of-stream", "End of Stream",
            "Start reading provenance Events from the end of the stream, ignoring old events");

    static final PropertyDescriptor PLATFORM = new PropertyDescriptor.Builder()
            .name("Platform")
            .displayName("Platform")
            .description("The value to use for the platform field in each provenance event.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .defaultValue("nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_EVENT_TYPE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-event-filter")
            .displayName("Event Type to Include")
            .description("Comma-separated list of event types that will be used to filter the provenance events sent by the reporting task. "
                    + "Available event types are " + Arrays.deepToString(ProvenanceEventType.values()) + ". If no filter is set, all the events are sent. If "
                    + "multiple filters are set, the filters are cumulative.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_EVENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-event-filter-exclude")
            .displayName("Event Type to Exclude")
            .description("Comma-separated list of event types that will be used to exclude the provenance events sent by the reporting task. "
                    + "Available event types are " + Arrays.deepToString(ProvenanceEventType.values()) + ". If no filter is set, all the events are sent. If "
                    + "multiple filters are set, the filters are cumulative. If an event type is included in Event Type to Include and excluded here, then the "
                    + "exclusion takes precedence and the event will not be sent.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_TYPE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-type-filter")
            .displayName("Component Type to Include")
            .description("Regular expression to filter the provenance events based on the component type. Only the events matching the regular "
                    + "expression will be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_TYPE_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-type-filter-exclude")
            .displayName("Component Type to Exclude")
            .description("Regular expression to exclude the provenance events based on the component type. The events matching the regular "
                    + "expression will not be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative. "
                    + "If a component type is included in Component Type to Include and excluded here, then the exclusion takes precedence and the event will not be sent.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_ID = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-id-filter")
            .displayName("Component ID to Include")
            .description("Comma-separated list of component UUID that will be used to filter the provenance events sent by the reporting task. If no "
                    + "filter is set, all the events are sent. If multiple filters are set, the filters are cumulative.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_ID_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-id-filter-exclude")
            .displayName("Component ID to Exclude")
            .description("Comma-separated list of component UUID that will be used to exclude the provenance events sent by the reporting task. If no "
                    + "filter is set, all the events are sent. If multiple filters are set, the filters are cumulative. If a component UUID is included in "
                    + "Component ID to Include and excluded here, then the exclusion takes precedence and the event will not be sent.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_NAME = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-name-filter")
            .displayName("Component Name to Include")
            .description("Regular expression to filter the provenance events based on the component name. Only the events matching the regular "
                    + "expression will be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor FILTER_COMPONENT_NAME_EXCLUDE = new PropertyDescriptor.Builder()
            .name("s2s-prov-task-name-filter-exclude")
            .displayName("Component Name to Exclude")
            .description("Regular expression to exclude the provenance events based on the component name. The events matching the regular "
                    + "expression will not be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative. "
                    + "If a component name is included in Component Name to Include and excluded here, then the exclusion takes precedence and the event will not be sent.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .build();

    static final PropertyDescriptor START_POSITION = new PropertyDescriptor.Builder()
            .name("start-position")
            .displayName("Start Position")
            .description("If the Reporting Task has never been run, or if its state has been reset by a user, specifies where in the stream of Provenance Events the Reporting Task should start")
            .allowableValues(BEGINNING_OF_STREAM, END_OF_STREAM)
            .defaultValue(BEGINNING_OF_STREAM.getValue())
            .required(true)
            .build();

    private volatile ProvenanceEventConsumer consumer;

    public SiteToSiteProvenanceReportingTask() throws IOException {
        final InputStream schema = getClass().getClassLoader().getResourceAsStream("schema-provenance.avsc");
        recordSchema = AvroTypeUtil.createSchema(new Schema.Parser().parse(schema));
    }

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) throws IOException {
        consumer = new ProvenanceEventConsumer();
        consumer.setStartPositionValue(context.getProperty(START_POSITION).getValue());
        consumer.setBatchSize(context.getProperty(BATCH_SIZE).asInteger());
        consumer.setLogger(getLogger());

        // initialize component type filtering
        consumer.setComponentTypeRegex(context.getProperty(FILTER_COMPONENT_TYPE).evaluateAttributeExpressions().getValue());
        consumer.setComponentTypeRegexExclude(context.getProperty(FILTER_COMPONENT_TYPE_EXCLUDE).evaluateAttributeExpressions().getValue());
        consumer.setComponentNameRegex(context.getProperty(FILTER_COMPONENT_NAME).evaluateAttributeExpressions().getValue());
        consumer.setComponentNameRegexExclude(context.getProperty(FILTER_COMPONENT_NAME_EXCLUDE).evaluateAttributeExpressions().getValue());

        final String[] targetEventTypes = StringUtils.stripAll(StringUtils.split(context.getProperty(FILTER_EVENT_TYPE).evaluateAttributeExpressions().getValue(), ','));
        if(targetEventTypes != null) {
            for(String type : targetEventTypes) {
                try {
                    consumer.addTargetEventType(ProvenanceEventType.valueOf(type));
                } catch (Exception e) {
                    getLogger().warn(type + " is not a correct event type, removed from the filtering.");
                }
            }
        }

        final String[] targetEventTypesExclude = StringUtils.stripAll(StringUtils.split(context.getProperty(FILTER_EVENT_TYPE_EXCLUDE).evaluateAttributeExpressions().getValue(), ','));
        if(targetEventTypesExclude != null) {
            for(String type : targetEventTypesExclude) {
                try {
                    consumer.addTargetEventTypeExclude(ProvenanceEventType.valueOf(type));
                } catch (Exception e) {
                    getLogger().warn(type + " is not a correct event type, removed from the exclude filtering.");
                }
            }
        }

        // initialize component ID filtering
        final String[] targetComponentIds = StringUtils.stripAll(StringUtils.split(context.getProperty(FILTER_COMPONENT_ID).evaluateAttributeExpressions().getValue(), ','));
        if(targetComponentIds != null) {
            consumer.addTargetComponentId(targetComponentIds);
        }

        final String[] targetComponentIdsExclude = StringUtils.stripAll(StringUtils.split(context.getProperty(FILTER_COMPONENT_ID_EXCLUDE).evaluateAttributeExpressions().getValue(), ','));
        if(targetComponentIdsExclude != null) {
            consumer.addTargetComponentIdExclude(targetComponentIdsExclude);
        }

        consumer.setScheduled(true);
    }

    @OnUnscheduled
    public void onUnscheduled() {
        if (consumer != null) {
            consumer.setScheduled(false);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PLATFORM);
        properties.add(FILTER_EVENT_TYPE);
        properties.add(FILTER_EVENT_TYPE_EXCLUDE);
        properties.add(FILTER_COMPONENT_TYPE);
        properties.add(FILTER_COMPONENT_TYPE_EXCLUDE);
        properties.add(FILTER_COMPONENT_ID);
        properties.add(FILTER_COMPONENT_ID_EXCLUDE);
        properties.add(FILTER_COMPONENT_NAME);
        properties.add(FILTER_COMPONENT_NAME_EXCLUDE);
        properties.add(START_POSITION);
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
        final JsonObjectBuilder builder = factory.createObjectBuilder();

        final DateFormat df = new SimpleDateFormat(TIMESTAMP_FORMAT);
        df.setTimeZone(TimeZone.getTimeZone("Z"));

        consumer.consumeEvents(context, (mapHolder, events) -> {
            final long start = System.nanoTime();
            // Create a JSON array of all the events in the current batch
            final JsonArrayBuilder arrayBuilder = factory.createArrayBuilder();
            for (final ProvenanceEventRecord event : events) {
                final String componentName = mapHolder.getComponentName(event.getComponentId());
                final String processGroupId = mapHolder.getProcessGroupId(event.getComponentId(), event.getComponentType());
                final String processGroupName = mapHolder.getComponentName(processGroupId);
                arrayBuilder.add(serialize(factory, builder, event, df, componentName, processGroupId, processGroupName, hostname, url, rootGroupName, platform, nodeId));
            }
            final JsonArray jsonArray = arrayBuilder.build();

            // Send the JSON document for the current batch
            try {
                final Transaction transaction = getClient().createTransaction(TransferDirection.SEND);
                if (transaction == null) {
                    // Throw an exception to avoid provenance event id will not proceed so that those can be consumed again.
                    throw new ProcessException("All destination nodes are penalized; will attempt to send data later");
                }

                final Map<String, String> attributes = new HashMap<>();
                final String transactionId = UUID.randomUUID().toString();
                attributes.put("reporting.task.transaction.id", transactionId);
                attributes.put("reporting.task.name", getName());
                attributes.put("reporting.task.uuid", getIdentifier());
                attributes.put("reporting.task.type", this.getClass().getSimpleName());
                attributes.put("mime.type", "application/json");

                sendData(context, transaction, attributes, jsonArray);
                transaction.confirm();
                transaction.complete();

                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                getLogger().info("Successfully sent {} Provenance Events to destination in {} ms; Transaction ID = {}; First Event ID = {}",
                        new Object[] {events.size(), transferMillis, transactionId, events.get(0).getEventId()});
            } catch (final IOException e) {
                throw new ProcessException("Failed to send Provenance Events to destination due to IOException:" + e.getMessage(), e);
            }
        });

    }


    private JsonObject serialize(final JsonBuilderFactory factory, final JsonObjectBuilder builder, final ProvenanceEventRecord event, final DateFormat df,
                                final String componentName, final String processGroupId, final String processGroupName, final String hostname, final URL nifiUrl, final String applicationName,
                                final String platform, final String nodeIdentifier) {
        addField(builder, "eventId", UUID.randomUUID().toString());
        addField(builder, "eventOrdinal", event.getEventId());
        addField(builder, "eventType", event.getEventType().name());
        addField(builder, "timestampMillis", event.getEventTime());
        addField(builder, "timestamp", df.format(event.getEventTime()));
        addField(builder, "durationMillis", event.getEventDuration());
        addField(builder, "lineageStart", event.getLineageStartDate());
        addField(builder, "details", event.getDetails());
        addField(builder, "componentId", event.getComponentId());
        addField(builder, "componentType", event.getComponentType());
        addField(builder, "componentName", componentName);
        addField(builder, "processGroupId", processGroupId, true);
        addField(builder, "processGroupName", processGroupName, true);
        addField(builder, "entityId", event.getFlowFileUuid());
        addField(builder, "entityType", "org.apache.nifi.flowfile.FlowFile");
        addField(builder, "entitySize", event.getFileSize());
        addField(builder, "previousEntitySize", event.getPreviousFileSize());
        addField(builder, factory, "updatedAttributes", event.getUpdatedAttributes());
        addField(builder, factory, "previousAttributes", event.getPreviousAttributes());

        addField(builder, "actorHostname", hostname);
        if (nifiUrl != null) {
            // TO get URL Prefix, we just remove the /nifi from the end of the URL. We know that the URL ends with
            // "/nifi" because the Property Validator enforces it
            final String urlString = nifiUrl.toString();
            final String urlPrefix = urlString.substring(0, urlString.length() - DESTINATION_URL_PATH.length());

            final String contentUriBase = urlPrefix + "/nifi-api/provenance-events/" + event.getEventId() + "/content/";
            final String nodeIdSuffix = nodeIdentifier == null ? "" : "?clusterNodeId=" + nodeIdentifier;
            addField(builder, "contentURI", contentUriBase + "output" + nodeIdSuffix);
            addField(builder, "previousContentURI", contentUriBase + "input" + nodeIdSuffix);
        }

        addField(builder, factory, "parentIds", event.getParentUuids());
        addField(builder, factory, "childIds", event.getChildUuids());
        addField(builder, "transitUri", event.getTransitUri());
        addField(builder, "remoteIdentifier", event.getSourceSystemFlowFileIdentifier());
        addField(builder, "alternateIdentifier", event.getAlternateIdentifierUri());
        addField(builder, "platform", platform);
        addField(builder, "application", applicationName);

        return builder.build();
    }

    private static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key, final Map<String, String> values) {
        if (values == null) {
            return;
        }

        final JsonObjectBuilder mapBuilder = factory.createObjectBuilder();
        for (final Map.Entry<String, String> entry : values.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null) {
                continue;
            }

            mapBuilder.add(entry.getKey(), entry.getValue());
        }

        builder.add(key, mapBuilder);
    }

    private void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key, final Collection<String> values) {
        if (values == null) {
            return;
        }

        builder.add(key, createJsonArray(factory, values));
    }

    private static JsonArrayBuilder createJsonArray(JsonBuilderFactory factory, final Collection<String> values) {
        final JsonArrayBuilder builder = factory.createArrayBuilder();
        for (final String value : values) {
            if (value != null) {
                builder.add(value);
            }
        }
        return builder;
    }
}
