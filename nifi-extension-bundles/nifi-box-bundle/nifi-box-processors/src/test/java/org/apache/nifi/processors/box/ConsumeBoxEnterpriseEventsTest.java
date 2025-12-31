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

import com.box.sdkgen.schemas.event.Event;
import com.box.sdkgen.schemas.event.EventEventTypeField;
import com.box.sdkgen.schemas.events.Events;
import com.box.sdkgen.schemas.events.EventsNextStreamPositionField;
import com.box.sdkgen.serialization.json.EnumWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.processors.box.ConsumeBoxEnterpriseEvents.StartEventPosition;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumeBoxEnterpriseEventsTest extends AbstractBoxFileTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Events mockEvents;
    private Events emptyEvents;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        // Create the empty events mock (returned after first call to break the loop)
        emptyEvents = mock(Events.class);
        when(emptyEvents.getEntries()).thenReturn(List.of());
        EventsNextStreamPositionField nextStreamPositionEmpty = mock(EventsNextStreamPositionField.class);
        when(nextStreamPositionEmpty.isString()).thenReturn(true);
        when(nextStreamPositionEmpty.getString()).thenReturn("end");
        when(emptyEvents.getNextStreamPosition()).thenReturn(nextStreamPositionEmpty);

        // Create a test subclass that overrides getEvents to use our mock data
        final AtomicInteger callCount = new AtomicInteger(0);
        final ConsumeBoxEnterpriseEventsTest testReference = this;

        final ConsumeBoxEnterpriseEvents testSubject = new ConsumeBoxEnterpriseEvents() {
            @Override
            Events getEvents(String position) {
                // Return events on first call, empty on subsequent calls to break the loop
                if (callCount.getAndIncrement() == 0) {
                    return testReference.mockEvents != null ? testReference.mockEvents : emptyEvents;
                }
                return emptyEvents;
            }
        };

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testConsumeEventsFromEarliest() throws Exception {
        testRunner.setProperty(ConsumeBoxEnterpriseEvents.START_EVENT_POSITION, StartEventPosition.EARLIEST);

        // Create mock events
        List<Event> events = new ArrayList<>();
        events.add(createMockEvent("1", EventEventTypeField.ITEM_CREATE));
        events.add(createMockEvent("2", EventEventTypeField.ITEM_TRASH));
        events.add(createMockEvent("3", EventEventTypeField.ITEM_UPLOAD));

        mockEvents = mock(Events.class);
        when(mockEvents.getEntries()).thenReturn(events);
        EventsNextStreamPositionField nextStreamPosition3 = mock(EventsNextStreamPositionField.class);
        when(nextStreamPosition3.isString()).thenReturn(true);
        when(nextStreamPosition3.getString()).thenReturn("3");
        when(mockEvents.getNextStreamPosition()).thenReturn(nextStreamPosition3);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ConsumeBoxEnterpriseEvents.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ConsumeBoxEnterpriseEvents.REL_SUCCESS).getFirst();

        // Parse and verify the content
        final String content = flowFile.getContent();
        final JsonNode jsonArray = OBJECT_MAPPER.readTree(content);
        assertEquals(3, jsonArray.size());

        assertEquals(3, testRunner.getCounterValue(ConsumeBoxEnterpriseEvents.COUNTER_RECORDS_PROCESSED));
    }

    @Test
    void testNoEventsReturned() {
        testRunner.setProperty(ConsumeBoxEnterpriseEvents.START_EVENT_POSITION, StartEventPosition.EARLIEST);

        // Set mockEvents to null so it returns emptyEvents
        mockEvents = null;

        testRunner.run();

        testRunner.assertTransferCount(ConsumeBoxEnterpriseEvents.REL_SUCCESS, 0);
    }

    private Event createMockEvent(String eventId, EventEventTypeField eventType) {
        Event event = mock(Event.class);
        when(event.getEventId()).thenReturn(eventId);
        when(event.getEventType()).thenReturn(new EnumWrapper<>(eventType));
        when(event.getCreatedAt()).thenReturn(null);
        when(event.getSessionId()).thenReturn(null);
        when(event.getCreatedBy()).thenReturn(null);
        when(event.getSource()).thenReturn(null);
        when(event.getAdditionalDetails()).thenReturn(null);
        return event;
    }
}
