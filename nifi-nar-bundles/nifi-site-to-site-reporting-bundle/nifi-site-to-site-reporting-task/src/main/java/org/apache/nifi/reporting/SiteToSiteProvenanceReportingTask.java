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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
import java.util.regex.Pattern;

@Tags({"provenance", "lineage", "tracking", "site", "site to site", "restricted"})
@CapabilityDescription("Publishes Provenance events using the Site To Site protocol.")
@Stateful(scopes = Scope.LOCAL, description = "Stores the Reporting Task's last event Id so that on restart the task knows where it left off.")
@Restricted("Provides operator the ability send sensitive details contained in Provenance events to any external system.")
public class SiteToSiteProvenanceReportingTask extends AbstractSiteToSiteReportingTask {

    static final String TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    static final String LAST_EVENT_ID_KEY = "last_event_id";

    static final PropertyDescriptor PLATFORM = new PropertyDescriptor.Builder()
        .name("Platform")
        .displayName("Platform")
        .description("The value to use for the platform field in each provenance event.")
        .required(true)
        .expressionLanguageSupported(true)
        .defaultValue("nifi")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor FILTER_EVENT_TYPE = new PropertyDescriptor.Builder()
        .name("s2s-prov-task-event-filter")
        .displayName("Event type")
        .description("Comma-separated list of event types that will be used to filter the provenance events sent by the reporting task. "
                + "Available event types are " + ProvenanceEventType.values() + ". If no filter is set, all the events are sent. If "
                        + "multiple filters are set, the filters are cumulative.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final PropertyDescriptor FILTER_COMPONENT_TYPE = new PropertyDescriptor.Builder()
        .name("s2s-prov-task-type-filter")
        .displayName("Component type")
        .description("Regular expression to filter the provenance events based on the component type. Only the events matching the regular "
                + "expression will be sent. If no filter is set, all the events are sent. If multiple filters are set, the filters are cumulative.")
        .required(false)
        .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
        .build();

    static final PropertyDescriptor FILTER_COMPONENT_ID = new PropertyDescriptor.Builder()
        .name("s2s-prov-task-id-filter")
        .displayName("Component ID")
        .description("Comma-separated list of component UUID that will be used to filter the provenance events sent by the reporting task. If no "
                + "filter is set, all the events are sent. If multiple filters are set, the filters are cumulative.")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private volatile long firstEventId = -1L;
    private volatile boolean isFilteringEnabled = false;
    private volatile Pattern componentTypeRegex;
    private volatile List<ProvenanceEventType> eventTypes = new ArrayList<ProvenanceEventType>();
    private volatile List<String> componentIds = new ArrayList<String>();

    @OnScheduled
    public void onScheduled(final ConfigurationContext context) throws IOException {
        // initialize component type filtering
        componentTypeRegex = StringUtils.isBlank(context.getProperty(FILTER_COMPONENT_TYPE).getValue()) ? null : Pattern.compile(context.getProperty(FILTER_COMPONENT_TYPE).getValue());

        final String[] eventList = StringUtils.stripAll(StringUtils.split(context.getProperty(FILTER_EVENT_TYPE).getValue(), ','));
        if(eventList != null) {
            for(String type : eventList) {
                try {
                    eventTypes.add(ProvenanceEventType.valueOf(type));
                } catch (Exception e) {
                    getLogger().warn(type + " is not a correct event type, removed from the filtering.");
                }
            }
        }

        // initialize component ID filtering
        final String[] componentIdList = StringUtils.stripAll(StringUtils.split(context.getProperty(FILTER_COMPONENT_ID).getValue(), ','));
        if(componentIdList != null) {
            componentIds.addAll(Arrays.asList(componentIdList));
        }

        // set a boolean whether filtering will be applied or not
        isFilteringEnabled = componentTypeRegex != null || !eventTypes.isEmpty() || !componentIds.isEmpty();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PLATFORM);
        properties.add(FILTER_EVENT_TYPE);
        properties.add(FILTER_COMPONENT_TYPE);
        properties.add(FILTER_COMPONENT_ID);
        return properties;
    }

    private Map<String,String> createComponentMap(final ProcessGroupStatus status) {
        final Map<String,String> componentMap = new HashMap<>();

        if (status != null) {
            componentMap.put(status.getId(), status.getName());

            for (final ProcessorStatus procStatus : status.getProcessorStatus()) {
                componentMap.put(procStatus.getId(), procStatus.getName());
            }

            for (final PortStatus portStatus : status.getInputPortStatus()) {
                componentMap.put(portStatus.getId(), portStatus.getName());
            }

            for (final PortStatus portStatus : status.getOutputPortStatus()) {
                componentMap.put(portStatus.getId(), portStatus.getName());
            }

            for (final RemoteProcessGroupStatus rpgStatus : status.getRemoteProcessGroupStatus()) {
                componentMap.put(rpgStatus.getId(), rpgStatus.getName());
            }

            for (final ProcessGroupStatus childGroup : status.getProcessGroupStatus()) {
                componentMap.put(childGroup.getId(), childGroup.getName());
            }
        }

        return componentMap;
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
        final Map<String,String> componentMap = createComponentMap(procGroupStatus);

        Long currMaxId = context.getEventAccess().getProvenanceRepository().getMaxEventId();

        if(currMaxId == null) {
            getLogger().debug("No events to send because no events have been created yet.");
            return;
        }

        if (firstEventId < 0) {
            Map<String, String> state;
            try {
                state = context.getStateManager().getState(Scope.LOCAL).toMap();
            } catch (IOException e) {
                getLogger().error("Failed to get state at start up due to:" + e.getMessage(), e);
                return;
            }
            if (state.containsKey(LAST_EVENT_ID_KEY)) {
                firstEventId = Long.parseLong(state.get(LAST_EVENT_ID_KEY)) + 1;
            }

            if(currMaxId < (firstEventId - 1)){
                getLogger().warn("Current provenance max id is {} which is less than what was stored in state as the last queried event, which was {}. This means the provenance restarted its " +
                        "ids. Restarting querying from the beginning.", new Object[]{currMaxId, firstEventId});
                firstEventId = -1;
            }
        }

        if (currMaxId == (firstEventId - 1)) {
            getLogger().debug("No events to send due to the current max id being equal to the last id that was queried.");
            return;
        }

        List<ProvenanceEventRecord> events;
        try {
            events = filterEvents(context.getEventAccess().getProvenanceEvents(firstEventId, context.getProperty(BATCH_SIZE).asInteger()));
        } catch (final IOException ioe) {
            getLogger().error("Failed to retrieve Provenance Events from repository due to: " + ioe.getMessage(), ioe);
            return;
        }

        if (events == null || events.isEmpty()) {
            getLogger().debug("No events to send due to 'events' being null or empty.");
            return;
        }

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

        while (events != null && !events.isEmpty()) {
            final long start = System.nanoTime();

            // Create a JSON array of all the events in the current batch
            final JsonArrayBuilder arrayBuilder = factory.createArrayBuilder();
            for (final ProvenanceEventRecord event : events) {
                final String componentName = componentMap.get(event.getComponentId());
                arrayBuilder.add(serialize(factory, builder, event, df, componentName, hostname, url, rootGroupName, platform, nodeId));
            }
            final JsonArray jsonArray = arrayBuilder.build();

            // Send the JSON document for the current batch
            try {
                final Transaction transaction = getClient().createTransaction(TransferDirection.SEND);
                if (transaction == null) {
                    getLogger().debug("All destination nodes are penalized; will attempt to send data later");
                    return;
                }

                final Map<String, String> attributes = new HashMap<>();
                final String transactionId = UUID.randomUUID().toString();
                attributes.put("reporting.task.transaction.id", transactionId);
                attributes.put("mime.type", "application/json");

                final byte[] data = jsonArray.toString().getBytes(StandardCharsets.UTF_8);
                transaction.send(data, attributes);
                transaction.confirm();
                transaction.complete();

                final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                getLogger().info("Successfully sent {} Provenance Events to destination in {} ms; Transaction ID = {}; First Event ID = {}",
                        new Object[]{events.size(), transferMillis, transactionId, events.get(0).getEventId()});
            } catch (final IOException e) {
                throw new ProcessException("Failed to send Provenance Events to destination due to IOException:" + e.getMessage(), e);
            }

            // Store the id of the last event so we know where we left off
            final ProvenanceEventRecord lastEvent = events.get(events.size() - 1);
            final String lastEventId = String.valueOf(lastEvent.getEventId());
            try {
                StateManager stateManager = context.getStateManager();
                Map<String, String> newMapOfState = new HashMap<>();
                newMapOfState.put(LAST_EVENT_ID_KEY, lastEventId);
                stateManager.setState(newMapOfState, Scope.LOCAL);
            } catch (final IOException ioe) {
                getLogger().error("Failed to update state to {} due to {}; this could result in events being re-sent after a restart. The message of {} was: {}",
                        new Object[]{lastEventId, ioe, ioe, ioe.getMessage()}, ioe);
            }

            firstEventId = lastEvent.getEventId() + 1;

            // Retrieve the next batch
            try {
                events = filterEvents(context.getEventAccess().getProvenanceEvents(firstEventId, context.getProperty(BATCH_SIZE).asInteger()));
            } catch (final IOException ioe) {
                getLogger().error("Failed to retrieve Provenance Events from repository due to: " + ioe.getMessage(), ioe);
                return;
            }
        }

    }

    private List<ProvenanceEventRecord> filterEvents(List<ProvenanceEventRecord> provenanceEvents) {
        if(isFilteringEnabled) {
            List<ProvenanceEventRecord> filteredEvents = new ArrayList<ProvenanceEventRecord>();

            for (ProvenanceEventRecord provenanceEventRecord : provenanceEvents) {
                if(!componentIds.isEmpty() && !componentIds.contains(provenanceEventRecord.getComponentId())) {
                    continue;
                }
                if(!eventTypes.isEmpty() && !eventTypes.contains(provenanceEventRecord.getEventType())) {
                    continue;
                }
                if(componentTypeRegex != null && !componentTypeRegex.matcher(provenanceEventRecord.getComponentType()).matches()) {
                    continue;
                }
                filteredEvents.add(provenanceEventRecord);
            }

            return filteredEvents;
        } else {
            return provenanceEvents;
        }
    }

    static JsonObject serialize(final JsonBuilderFactory factory, final JsonObjectBuilder builder, final ProvenanceEventRecord event, final DateFormat df,
        final String componentName, final String hostname, final URL nifiUrl, final String applicationName, final String platform, final String nodeIdentifier) {
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

    private static void addField(final JsonObjectBuilder builder, final String key, final Long value) {
        if (value != null) {
            builder.add(key, value.longValue());
        }
    }

    private static void addField(final JsonObjectBuilder builder, final JsonBuilderFactory factory, final String key, final Collection<String> values) {
        if (values == null) {
            return;
        }

        builder.add(key, createJsonArray(factory, values));
    }

    private static void addField(final JsonObjectBuilder builder, final String key, final String value) {
        if (value == null) {
            return;
        }

        builder.add(key, value);
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
