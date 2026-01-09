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

import com.box.sdkgen.client.BoxClient;
import com.box.sdkgen.managers.events.GetEventsQueryParams;
import com.box.sdkgen.managers.events.GetEventsQueryParamsStreamTypeField;
import com.box.sdkgen.schemas.event.Event;
import com.box.sdkgen.schemas.events.Events;
import com.box.sdkgen.schemas.events.EventsNextStreamPositionField;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@PrimaryNodeOnly
@TriggerSerially
@Tags({"box", "storage"})
@CapabilityDescription("""
        Consumes all events from Box. This processor can be used to capture events such as uploads, modifications, deletions, etc.
        The content of the events is sent to the 'success' relationship as a JSON array.
        The last known position of the Box stream is stored in the processor state and is used to
        resume the stream from the last known position when the processor is restarted.
        """)
@SeeAlso({ FetchBoxFile.class, PutBoxFile.class, ListBoxFile.class })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Stateful(description = """
        The last known position of the Box stream is stored in the processor state and is used to
        resume the stream from the last known position when the processor is restarted.
        """, scopes = { Scope.CLUSTER })
public class ConsumeBoxEvents extends AbstractBoxProcessor implements VerifiableProcessor {

    private static final String POSITION_KEY = "position";

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOX_CLIENT_SERVICE
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Events received successfully will be sent out this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private volatile BoxClient boxClient;
    private volatile String streamPosition;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.removeProperty("Queue Capacity");
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final BoxClientService boxClientService = context.getProperty(BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);
        boxClient = boxClientService.getBoxClient();

        try {
            final String savedPosition = context.getStateManager().getState(Scope.CLUSTER).get(POSITION_KEY);
            streamPosition = savedPosition != null ? savedPosition : "0";
        } catch (Exception e) {
            throw new ProcessException("Could not retrieve last event position", e);
        }
    }

    /**
     * Extracts the stream position value from EventsNextStreamPositionField.
     * The field can contain either a String or Long value.
     */
    private String extractStreamPosition(final EventsNextStreamPositionField positionField) {
        if (positionField == null) {
            return null;
        }
        if (positionField.isString()) {
            return positionField.getString();
        } else if (positionField.isLongNumber()) {
            return String.valueOf(positionField.getLongNumber());
        }
        throw new IllegalStateException("EventsNextStreamPositionField contains neither String nor Long value");
    }

    @Override
    public List<ConfigVerificationResult> verify(ProcessContext context, ComponentLog verificationLogger, Map<String, String> attributes) {

        final List<ConfigVerificationResult> results = new ArrayList<>();
        BoxClientService boxClientService = context.getProperty(BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);
        boxClient = boxClientService.getBoxClient();

        try {
            // Try to get events to verify the connection
            final GetEventsQueryParams queryParams = new GetEventsQueryParams.Builder()
                    .limit(1L)
                    .streamType(GetEventsQueryParamsStreamTypeField.ALL)
                    .build();
            boxClient.getEvents().getEvents(queryParams);

            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Box API Connection")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully validated Box connection")
                    .build());
        } catch (Exception e) {
            getLogger().warn("Failed to verify configuration", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Box API Connection")
                    .outcome(Outcome.FAILED)
                    .explanation(String.format("Failed to validate Box connection: %s", e.getMessage()))
                    .build());
        }

        return results;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        getLogger().debug("Polling Box Events from position: {}", streamPosition);

        final Events events;
        try {
            final GetEventsQueryParams queryParams = new GetEventsQueryParams.Builder()
                    .streamPosition(streamPosition)
                    .streamType(GetEventsQueryParamsStreamTypeField.ALL)
                    .build();
            events = boxClient.getEvents().getEvents(queryParams);
        } catch (Exception e) {
            getLogger().error("Failed to poll Box events from position {}", streamPosition, e);
            context.yield();
            return;
        }

        final String newPosition = extractStreamPosition(events.getNextStreamPosition());
        if (newPosition != null) {
            streamPosition = newPosition;
            try {
                context.getStateManager().setState(Map.of(POSITION_KEY, newPosition), Scope.CLUSTER);
            } catch (IOException e) {
                getLogger().warn("Failed to save position {} in processor state", newPosition, e);
            }
        }

        final List<Event> eventEntries = events.getEntries();
        if (eventEntries == null || eventEntries.isEmpty()) {
            context.yield();
            return;
        }

        final int recordCount = eventEntries.size();
        getLogger().debug("Consumed {} Box Events. New position: {}", recordCount, streamPosition);

        final FlowFile flowFile = session.create();
        try (final OutputStream out = session.write(flowFile);
             final BoxEventJsonArrayWriter writer = BoxEventJsonArrayWriter.create(out)) {
            for (Event event : eventEntries) {
                writer.write(event);
            }
        } catch (Exception e) {
            getLogger().error("Failed to write events to FlowFile", e);
            session.remove(flowFile);
            context.yield();
            return;
        }

        session.putAttribute(flowFile, "record.count", String.valueOf(recordCount));
        session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.transfer(flowFile, REL_SUCCESS);
    }
}
