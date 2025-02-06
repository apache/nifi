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
import com.box.sdk.EventListener;
import com.box.sdk.EventStream;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

@PrimaryNodeOnly
@TriggerSerially
@Tags({"box", "storage"})
@CapabilityDescription("""
        Captures all events from Box. This processor can be used to capture events such as uploads, modifications, deletions, etc.
        The content of the events is sent to the 'success' relationship as a JSON array.
        """)
@SeeAlso({ FetchBoxFile.class, PutBoxFile.class, ListBoxFile.class })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
public class CaptureBoxEvents extends AbstractProcessor implements VerifiableProcessor {

    public static final PropertyDescriptor MAX_MESSAGE_QUEUE_SIZE = new PropertyDescriptor.Builder()
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
            BoxClientService.BOX_CLIENT_SERVICE,
            MAX_MESSAGE_QUEUE_SIZE
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Events received successfully will be sent out this relationship.")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private volatile BoxAPIConnection boxAPIConnection;
    private volatile EventStream eventStream;
    protected volatile LinkedBlockingQueue<BoxEvent> events;

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
        final BoxClientService boxClientService = context.getProperty(BoxClientService.BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);
        boxAPIConnection = boxClientService.getBoxApiConnection();
        eventStream = new EventStream(boxAPIConnection);

        final int eventsCapacity = context.getProperty(MAX_MESSAGE_QUEUE_SIZE).asInteger();
        events = new LinkedBlockingQueue<>(eventsCapacity);

        eventStream.addListener(new EventListener() {

            private final AtomicLong position = new AtomicLong(0);

            @Override
            public void onEvent(BoxEvent event) {
                if (!events.offer(event)) {
                    getLogger().warn("Failed to add event (ID = {}) to queue. Queue is full.", event.getID());
                }
            }

            @Override
            public void onNextPosition(long pos) {
                position.set(pos);
                getLogger().debug("Next position: {}", position);
            }

            @Override
            public boolean onException(Throwable e) {
                getLogger().warn("An error has been received from the stream. Last tracked position {}", position.get(), e);
                return true;
            }
        });

        eventStream.start();
    }

    @OnStopped
    public void stopped() {
        if (eventStream != null && eventStream.isStarted()) {
            eventStream.stop();
            events.clear();
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(ProcessContext context, ComponentLog verificationLogger, Map<String, String> attributes) {

        final List<ConfigVerificationResult> results = new ArrayList<>();
        BoxClientService boxClientService = context.getProperty(BoxClientService.BOX_CLIENT_SERVICE).asControllerService(BoxClientService.class);
        boxAPIConnection = boxClientService.getBoxApiConnection();

        try {
            boxAPIConnection.refresh();
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
        final List<BoxEvent> boxEvents = new ArrayList<>();
        final int recordCount = events.drainTo(boxEvents);

        try (final OutputStream out = session.write(flowFile)) {
            final Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
            writer.write("[");
            final Iterator<BoxEvent> iterator = boxEvents.iterator();
            while (iterator.hasNext()) {
                BoxEvent event = iterator.next();
                JsonObject jsonEvent = toRecord(event);
                jsonEvent.writeTo(writer);
                if (iterator.hasNext()) {
                    writer.write(",");
                }
            }
            writer.write("]");
            writer.flush();
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

    private JsonObject toRecord(BoxEvent event) {
        JsonObject json = Json.object();

        json.add("accessibleBy", event.getAccessibleBy() == null ? Json.NULL : Json.parse(event.getAccessibleBy().getJson()));
        json.add("actionBy", event.getActionBy() == null ? Json.NULL : Json.parse(event.getActionBy().getJson()));
        json.add("additionalDetails", Objects.requireNonNullElse(event.getAdditionalDetails(), Json.NULL));
        json.add("createdAt", event.getCreatedAt() == null ? Json.NULL : Json.value(event.getCreatedAt().toString()));
        json.add("createdBy", event.getCreatedBy() == null ? Json.NULL : Json.parse(event.getCreatedBy().getJson()));
        json.add("eventType", event.getEventType() == null ? Json.NULL : Json.value(event.getEventType().name()));
        json.add("id", Objects.requireNonNullElse(Json.value(event.getID()), Json.NULL));
        json.add("ipAddress", Objects.requireNonNullElse(Json.value(event.getIPAddress()), Json.NULL));
        json.add("sessionID", Objects.requireNonNullElse(Json.value(event.getSessionID()), Json.NULL));
        json.add("source", Objects.requireNonNullElse(event.getSourceJSON(), Json.NULL));
        json.add("typeName", Objects.requireNonNullElse(Json.value(event.getTypeName()), Json.NULL));

        return json;
    }

}
