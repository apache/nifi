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
package org.apache.nifi.processors.box;

import com.box.sdk.BoxAPIConnection;
import com.box.sdk.BoxEvent;
import com.box.sdk.EnterpriseEventsStreamRequest;
import com.box.sdk.EventLog;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.annotation.behavior.InputRequirement.Requirement.INPUT_FORBIDDEN;

@PrimaryNodeOnly
@TriggerSerially
@Tags({"box", "storage"})
@CapabilityDescription("""
        Consumes Enterprise Events from Box admin_logs_streaming Stream Type.
        The content of the events is sent to the 'success' relationship as a JSON array.
        The last known position of the Box stream is stored in the processor state and is used to
        resume the stream from the last known position when the processor is restarted.
        """)
@SeeAlso({ ConsumeBoxEvents.class, FetchBoxFile.class, ListBoxFile.class })
@InputRequirement(INPUT_FORBIDDEN)
@Stateful(description = """
        The last known position of the Box Event stream is stored in the processor state and is used to
        resume the stream from the last known position when the processor is restarted.
        """, scopes = { Scope.CLUSTER })
public class ConsumeBoxEnterpriseEvents extends AbstractProcessor {

    private static final String POSITION_KEY = "position";
    private static final String EARLIEST_POSITION = "0";
    private static final String LATEST_POSITION = "now";

    private static final int LIMIT = 500;

    static final String COUNTER_RECORDS_PROCESSED = "Records Processed";

    static final PropertyDescriptor EVENT_TYPES = new PropertyDescriptor.Builder()
            .name("Event Types")
            .description("A comma separated list of Enterprise Events to consume. If not set, all Events are consumed." +
                    "See Additional Details for more information.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor START_EVENT_POSITION = new PropertyDescriptor.Builder()
            .name("Start Event Position")
            .description("What position to consume the Events from.")
            .required(true)
            .allowableValues(StartEventPosition.class)
            .defaultValue(StartEventPosition.EARLIEST)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    static final PropertyDescriptor START_OFFSET = new PropertyDescriptor.Builder()
            .name("Start Offset")
            .description("The offset to start consuming the Events from.")
            .required(true)
            .dependsOn(START_EVENT_POSITION, StartEventPosition.OFFSET)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BoxClientService.BOX_CLIENT_SERVICE,
            EVENT_TYPES,
            START_EVENT_POSITION,
            START_OFFSET
    );

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Events received successfully will be sent out this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    private volatile BoxAPIConnection boxAPIConnection;
    private volatile String[] eventTypes;
    private volatile String streamPosition;

    @OnScheduled
    public void onEnabled(final ProcessContext context) {
        final BoxClientService boxClientService = context.getProperty(BoxClientService.BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);
        boxAPIConnection = boxClientService.getBoxApiConnection();

        eventTypes = context.getProperty(EVENT_TYPES).isSet()
                ? context.getProperty(EVENT_TYPES).getValue().split(",")
                : new String[0];

        streamPosition = calculateStreamPosition(context);
    }

    private String calculateStreamPosition(final ProcessContext context) {
        return readStreamPosition(context)
                .orElseGet(() -> initializeStartEventPosition(context));
    }

    private Optional<String> readStreamPosition(final ProcessContext context) {
        try {
            final String position = context.getStateManager().getState(Scope.CLUSTER).get(POSITION_KEY);
            return Optional.ofNullable(position);
        } catch (final IOException e) {
            throw new ProcessException("Could not retrieve saved event position", e);
        }
    }

    private void writeStreamPosition(final String position, final ProcessSession session) {
        try {
            final Map<String, String> stateMap = Map.of(POSITION_KEY, position);
            session.setState(stateMap, Scope.CLUSTER);
        } catch (final IOException e) {
            throw new ProcessException("Could not save event position", e);
        }
    }

    private String initializeStartEventPosition(final ProcessContext context) {
        final StartEventPosition startEventPosition = context.getProperty(START_EVENT_POSITION).asAllowableValue(StartEventPosition.class);
        return switch (startEventPosition) {
            case EARLIEST -> EARLIEST_POSITION;
            case LATEST -> retrieveLatestStreamPosition();
            case OFFSET -> context.getProperty(START_OFFSET).getValue();
        };
    }

    private String retrieveLatestStreamPosition() {
        final EventLog eventLog = getEventLog(LATEST_POSITION);
        return eventLog.getNextStreamPosition();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        while (isScheduled()) {
            getLogger().debug("Consuming Box Events from position: {}", streamPosition);

            final EventLog eventLog = getEventLog(streamPosition);
            streamPosition = eventLog.getNextStreamPosition();

            getLogger().debug("Consumed {} Box Enterprise Events. New position: {}", eventLog.getSize(), streamPosition);

            writeStreamPosition(streamPosition, session);

            if (eventLog.getSize() == 0) {
                break;
            }

            writeLogAsRecords(eventLog, session);
        }
    }

    // Package-private for testing.
    EventLog getEventLog(final String position) {
        final EnterpriseEventsStreamRequest request = new EnterpriseEventsStreamRequest()
                .limit(LIMIT)
                .position(position)
                .typeNames(eventTypes);

        return EventLog.getEnterpriseEventsStream(boxAPIConnection, request);
    }

    private void writeLogAsRecords(final EventLog eventLog, final ProcessSession session) {
        final FlowFile flowFile = session.create();

        try (final OutputStream out = session.write(flowFile);
             final BoxEventJsonArrayWriter writer = BoxEventJsonArrayWriter.create(out)) {

            for (final BoxEvent event : eventLog) {
                writer.write(event);
            }
        } catch (final IOException e) {
            throw new ProcessException("Failed to write Box Event into a FlowFile", e);
        }

        session.adjustCounter(COUNTER_RECORDS_PROCESSED, eventLog.getSize(), false);
        session.putAttribute(flowFile, "record.count", String.valueOf(eventLog.getSize()));
        session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.transfer(flowFile, REL_SUCCESS);
    }

    public enum StartEventPosition implements DescribedValue {
        EARLIEST("earliest", "Start consuming events from the earliest available Event."),
        LATEST("latest", "Start consuming events from the latest Event."),
        OFFSET("offset", "Start consuming events from the specified offset.");

        private final String value;
        private final String description;

        StartEventPosition(final String value, final String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String getDisplayName() {
            return value;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }
}
