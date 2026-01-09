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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.processors.box.ConsumeBoxEnterpriseEvents.StartEventPosition;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ConsumeBoxEnterpriseEventsTest extends AbstractBoxFileTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private Events testEvents;
    private final Events emptyEvents = new Events.Builder()
            .entries(List.of())
            .nextStreamPosition("end")
            .build();

    @Override
    @BeforeEach
    void setUp() throws Exception {
        // Create a test subclass that overrides getEvents to use our test data
        final AtomicInteger callCount = new AtomicInteger(0);
        final ConsumeBoxEnterpriseEventsTest testReference = this;

        final ConsumeBoxEnterpriseEvents testSubject = new ConsumeBoxEnterpriseEvents() {
            @Override
            Events getEvents(String position) {
                // Return events on first call, empty on subsequent calls to break the loop
                if (callCount.getAndIncrement() == 0) {
                    return testReference.testEvents != null ? testReference.testEvents : emptyEvents;
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

        // Create events using real SDK objects
        List<Event> events = List.of(
                createEvent("1", EventEventTypeField.ITEM_CREATE),
                createEvent("2", EventEventTypeField.ITEM_TRASH),
                createEvent("3", EventEventTypeField.ITEM_UPLOAD)
        );

        testEvents = new Events.Builder()
                .entries(events)
                .nextStreamPosition("3")
                .build();

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

        // Set testEvents to null so it returns emptyEvents
        testEvents = null;

        testRunner.run();

        testRunner.assertTransferCount(ConsumeBoxEnterpriseEvents.REL_SUCCESS, 0);
    }

    private Event createEvent(String eventId, EventEventTypeField eventType) {
        return new Event.Builder()
                .eventId(eventId)
                .eventType(eventType)
                .build();
    }
}
