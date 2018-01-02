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
package org.apache.nifi.reporting.util.provenance;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.EventAccess;
import org.apache.nifi.reporting.ReportingContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

public class ProvenanceEventConsumer {

    public static final String LAST_EVENT_ID_KEY = "last_event_id";

    public static final AllowableValue BEGINNING_OF_STREAM = new AllowableValue("beginning-of-stream", "Beginning of Stream",
            "Start reading provenance Events from the beginning of the stream (the oldest event first)");
    public static final AllowableValue END_OF_STREAM = new AllowableValue("end-of-stream", "End of Stream",
            "Start reading provenance Events from the end of the stream, ignoring old events");
    public static final PropertyDescriptor PROVENANCE_START_POSITION = new PropertyDescriptor.Builder()
            .name("provenance-start-position")
            .displayName("Provenance Record Start Position")
            .description("If the Reporting Task has never been run, or if its state has been reset by a user, specifies where in the stream of Provenance Events the Reporting Task should start")
            .allowableValues(BEGINNING_OF_STREAM, END_OF_STREAM)
            .defaultValue(BEGINNING_OF_STREAM.getValue())
            .required(true)
            .build();
    public static final PropertyDescriptor PROVENANCE_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("provenance-batch-size")
            .displayName("Provenance Record Batch Size")
            .description("Specifies how many records to send in a single batch, at most.")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();


    private String startPositionValue = PROVENANCE_START_POSITION.getDefaultValue();
    private Pattern componentTypeRegex;
    private List<ProvenanceEventType> eventTypes = new ArrayList<ProvenanceEventType>();
    private List<String> componentIds = new ArrayList<String>();
    private int batchSize = Integer.parseInt(PROVENANCE_BATCH_SIZE.getDefaultValue());

    private volatile long firstEventId = -1L;
    private volatile boolean scheduled = false;

    private ComponentLog logger;

    public void setStartPositionValue(String startPositionValue) {
        this.startPositionValue = startPositionValue;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setComponentTypeRegex(final String componentTypeRegex) {
        if (!StringUtils.isBlank(componentTypeRegex)) {
            this.componentTypeRegex = Pattern.compile(componentTypeRegex);
        }
    }

    public void addTargetEventType(final ProvenanceEventType... types) {
        for (ProvenanceEventType type : types) {
            eventTypes.add(type);
        }
    }

    public void addTargetComponentId(final String... ids) {
        for (String id : ids) {
            componentIds.add(id);
        }
    }

    public void setScheduled(boolean scheduled) {
        this.scheduled = scheduled;
    }

    public boolean isScheduled() {
        return scheduled;
    }

    public void setLogger(ComponentLog logger) {
        this.logger = logger;
    }

    public void consumeEvents(final ReportingContext context,
                              final BiConsumer<ComponentMapHolder, List<ProvenanceEventRecord>> consumer) throws ProcessException {

        if (context == null) {
            logger.debug("No ReportingContext available.");
            return;
        }
        final EventAccess eventAccess = context.getEventAccess();
        final ProcessGroupStatus procGroupStatus = eventAccess.getControllerStatus();
        final ComponentMapHolder componentMapHolder = ComponentMapHolder.createComponentMap(procGroupStatus);
        final StateManager stateManager = context.getStateManager();

        Long currMaxId = eventAccess.getProvenanceRepository().getMaxEventId();

        if (currMaxId == null) {
            logger.debug("No events to send because no events have been created yet.");
            return;
        }

        if (firstEventId < 0) {
            Map<String, String> state;
            try {
                state = stateManager.getState(Scope.LOCAL).toMap();
            } catch (IOException e) {
                logger.error("Failed to get state at start up due to:" + e.getMessage(), e);
                return;
            }

            if (state.containsKey(LAST_EVENT_ID_KEY)) {
                firstEventId = Long.parseLong(state.get(LAST_EVENT_ID_KEY)) + 1;
            } else {
                if (END_OF_STREAM.getValue().equals(startPositionValue)) {
                    firstEventId = currMaxId;
                }
            }

            if (currMaxId < (firstEventId - 1)) {
                if (BEGINNING_OF_STREAM.getValue().equals(startPositionValue)) {
                    logger.warn("Current provenance max id is {} which is less than what was stored in state as the last queried event, which was {}. This means the provenance restarted its " +
                            "ids. Restarting querying from the beginning.", new Object[]{currMaxId, firstEventId});
                    firstEventId = -1;
                } else {
                    logger.warn("Current provenance max id is {} which is less than what was stored in state as the last queried event, which was {}. This means the provenance restarted its " +
                            "ids. Restarting querying from the latest event in the Provenance Repository.", new Object[]{currMaxId, firstEventId});
                    firstEventId = currMaxId;
                }
            }
        }

        if (currMaxId == (firstEventId - 1)) {
            logger.debug("No events to send due to the current max id being equal to the last id that was queried.");
            return;
        }

        List<ProvenanceEventRecord> rawEvents;
        List<ProvenanceEventRecord> filteredEvents;
        try {
            rawEvents = eventAccess.getProvenanceEvents(firstEventId, batchSize);
            filteredEvents = filterEvents(componentMapHolder, rawEvents);
        } catch (final IOException ioe) {
            logger.error("Failed to retrieve Provenance Events from repository due to: " + ioe.getMessage(), ioe);
            return;
        }

        if (rawEvents == null || rawEvents.isEmpty()) {
            logger.debug("No events to send due to 'events' being null or empty.");
            return;
        }

        // Consume while there are more events and not stopped.
        while (rawEvents != null && !rawEvents.isEmpty() && isScheduled()) {

            if (!filteredEvents.isEmpty()) {
                // Executes callback.
                consumer.accept(componentMapHolder, filteredEvents);
            }

            firstEventId = updateLastEventId(rawEvents, stateManager);

            // Retrieve the next batch
            try {
                rawEvents = eventAccess.getProvenanceEvents(firstEventId, batchSize);
                filteredEvents = filterEvents(componentMapHolder, rawEvents);
            } catch (final IOException ioe) {
                logger.error("Failed to retrieve Provenance Events from repository due to: " + ioe.getMessage(), ioe);
                return;
            }
        }

    }

    private long updateLastEventId(final List<ProvenanceEventRecord> events, final StateManager stateManager) {
        if (events == null || events.isEmpty()) {
            return firstEventId;
        }

        // Store the id of the last event so we know where we left off
        final ProvenanceEventRecord lastEvent = events.get(events.size() - 1);
        final String lastEventId = String.valueOf(lastEvent.getEventId());
        try {
            Map<String, String> newMapOfState = new HashMap<>();
            newMapOfState.put(LAST_EVENT_ID_KEY, lastEventId);
            stateManager.setState(newMapOfState, Scope.LOCAL);
        } catch (final IOException ioe) {
            logger.error("Failed to update state to {} due to {}; this could result in events being re-sent after a restart. The message of {} was: {}",
                    new Object[]{lastEventId, ioe, ioe, ioe.getMessage()}, ioe);
        }

        return lastEvent.getEventId() + 1;
    }


    private boolean isFilteringEnabled() {
        return componentTypeRegex != null || !eventTypes.isEmpty() || !componentIds.isEmpty();
    }

    private List<ProvenanceEventRecord> filterEvents(ComponentMapHolder componentMapHolder, List<ProvenanceEventRecord> provenanceEvents) {
        if (isFilteringEnabled()) {
            List<ProvenanceEventRecord> filteredEvents = new ArrayList<>();

            for (ProvenanceEventRecord provenanceEventRecord : provenanceEvents) {
                final String componentId = provenanceEventRecord.getComponentId();
                if (!componentIds.isEmpty() && !componentIds.contains(componentId)) {
                    // If we aren't filtering it out based on component ID, let's see if this component has a parent process group IDs
                    // that is being filtered on
                    if (componentMapHolder == null) {
                        continue;
                    }
                    final String processGroupId = componentMapHolder.getProcessGroupId(componentId, provenanceEventRecord.getComponentType());
                    if (StringUtils.isEmpty(processGroupId)) {
                        continue;
                    }
                    // Check if the process group or any parent process group is specified as a target component ID.
                    if (!componentIds.contains(processGroupId)) {
                        ParentProcessGroupSearchNode parentProcessGroup = componentMapHolder.getProcessGroupParent(processGroupId);
                        while (parentProcessGroup != null && !componentIds.contains(parentProcessGroup.getId())) {
                            parentProcessGroup = parentProcessGroup.getParent();
                        }
                        if (parentProcessGroup == null) {
                            continue;
                        }
                    }
                }
                if (!eventTypes.isEmpty() && !eventTypes.contains(provenanceEventRecord.getEventType())) {
                    continue;
                }
                if (componentTypeRegex != null && !componentTypeRegex.matcher(provenanceEventRecord.getComponentType()).matches()) {
                    continue;
                }
                filteredEvents.add(provenanceEventRecord);
            }

            return filteredEvents;
        } else {
            return provenanceEvents;
        }
    }

}
