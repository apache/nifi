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
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.box.controllerservices.BoxClientService;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@PrimaryNodeOnly
@TriggerSerially
@Tags({"box", "storage"})
@CapabilityDescription("""
        Consumes all events from Box. This processor can be used to capture events such as uploads, modifications, deletions, etc.
        The content of the events is sent to the 'success' relationship as a JSON array. Events can be dropped in case of NiFi restart
        or if the queue capacity is exceeded. The last known position of the Box stream is stored in the processor state and is used to
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
    private static final long POLL_INTERVAL_MS = 5000;

    public static final PropertyDescriptor QUEUE_CAPACITY = new PropertyDescriptor.Builder()
            .name("Queue Capacity")
            .description("""
                    The maximum size of the internal queue used to buffer events being transferred from the underlying stream to the processor.
                    Setting this value higher allows more messages to be buffered in memory during surges of incoming messages, but increases the total
                    memory used by the processor during these surges.
                    """)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BOX_CLIENT_SERVICE,
            QUEUE_CAPACITY
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Events received successfully will be sent out this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private volatile BoxClient boxClient;
    protected volatile BlockingQueue<Event> events;
    private volatile AtomicReference<String> position = new AtomicReference<>("0");
    private volatile ScheduledExecutorService pollingExecutor;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public final Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final BoxClientService boxClientService = context.getProperty(BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);
        boxClient = boxClientService.getBoxClient();

        try {
            final String savedPosition = context.getStateManager().getState(Scope.CLUSTER).get(POSITION_KEY);
            if (savedPosition != null) {
                position.set(savedPosition);
            }
        } catch (Exception e) {
            throw new ProcessException("Could not retrieve last event position", e);
        }

        final int queueCapacity = context.getProperty(QUEUE_CAPACITY).asInteger();
        if (events == null) {
            events = new LinkedBlockingQueue<>(queueCapacity);
        } else {
            // create new one with events from the old queue in case capacity has changed
            final BlockingQueue<Event> newQueue = new LinkedBlockingQueue<>(queueCapacity);
            newQueue.addAll(events);
            events = newQueue;
        }

        // Start polling for events in a background thread
        pollingExecutor = Executors.newSingleThreadScheduledExecutor();
        pollingExecutor.scheduleWithFixedDelay(() -> {
            try {
                pollEvents(context);
            } catch (Exception e) {
                getLogger().warn("Error polling Box events", e);
            }
        }, 0, POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void pollEvents(final ProcessContext context) {
        try {
            final GetEventsQueryParams queryParams = new GetEventsQueryParams.Builder()
                    .streamPosition(position.get())
                    .streamType(GetEventsQueryParamsStreamTypeField.ALL)
                    .build();

            final Events eventResult = boxClient.getEvents().getEvents(queryParams);

            if (eventResult.getEntries() != null) {
                for (Event event : eventResult.getEntries()) {
                    events.offer(event);
                }
            }

            final String newPosition = extractStreamPosition(eventResult.getNextStreamPosition());
            if (newPosition != null) {
                position.set(newPosition);
                try {
                    context.getStateManager().setState(Map.of(POSITION_KEY, newPosition), Scope.CLUSTER);
                } catch (IOException e) {
                    getLogger().warn("Failed to save position {} in processor state", newPosition, e);
                }
            }
        } catch (Exception e) {
            getLogger().warn("An error occurred while polling Box events. Last tracked position {}", position.get(), e);
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
        return null;
    }

    @OnStopped
    public void stopped() {
        if (pollingExecutor != null && !pollingExecutor.isShutdown()) {
            pollingExecutor.shutdownNow();
        }
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
        if (events.isEmpty()) {
            context.yield();
            return;
        }

        final FlowFile flowFile = session.create();
        final List<Event> boxEvents = new ArrayList<>();
        final int recordCount = events.drainTo(boxEvents);

        try (final OutputStream out = session.write(flowFile);
             final BoxEventJsonArrayWriter writer = BoxEventJsonArrayWriter.create(out)) {
            for (Event event : boxEvents) {
                writer.write(event);
            }
        } catch (Exception e) {
            getLogger().error("Failed to write events to FlowFile; will re-queue events and try again", e);
            boxEvents.forEach(events::offer);
            session.remove(flowFile);
            context.yield();
            return;
        }

        session.putAttribute(flowFile, "record.count", String.valueOf(recordCount));
        session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.transfer(flowFile, REL_SUCCESS);
    }
}
