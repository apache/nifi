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

import com.box.sdkgen.managers.events.EventsManager;
import com.box.sdkgen.managers.events.GetEventsQueryParams;
import com.box.sdkgen.schemas.event.Event;
import com.box.sdkgen.schemas.event.EventEventTypeField;
import com.box.sdkgen.schemas.events.Events;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ConsumeBoxEventsTest extends AbstractBoxFileTest {

    @Mock
    private EventsManager mockEventsManager;

    @Override
    @BeforeEach
    void setUp() throws Exception {
        testRunner = TestRunners.newTestRunner(ConsumeBoxEvents.class);
        super.setUp();

        when(mockBoxClient.getEvents()).thenReturn(mockEventsManager);
    }

    @Test
    void testCaptureEvents() {
        // Create events using real SDK objects
        Event event1 = new Event.Builder()
                .eventId("1")
                .eventType(EventEventTypeField.ITEM_CREATE)
                .build();

        Event event2 = new Event.Builder()
                .eventId("2")
                .eventType(EventEventTypeField.ITEM_TRASH)
                .build();

        Events events = new Events.Builder()
                .entries(List.of(event1, event2))
                .nextStreamPosition("2")
                .build();

        when(mockEventsManager.getEvents(any(GetEventsQueryParams.class))).thenReturn(events);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ConsumeBoxEvents.REL_SUCCESS, 1);
        final MockFlowFile ff0 = testRunner.getFlowFilesForRelationship(ConsumeBoxEvents.REL_SUCCESS).getFirst();
        ff0.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        ff0.assertAttributeEquals("record.count", "2");

        final String content = ff0.getContent();
        assertTrue(content.contains("\"id\":\"1\""));
        assertTrue(content.contains("\"id\":\"2\""));
        assertTrue(content.contains("\"eventType\":\"ITEM_CREATE\""));
        assertTrue(content.contains("\"eventType\":\"ITEM_TRASH\""));
    }

    @Test
    void testNoEventsReturned() {
        Events emptyEvents = new Events.Builder()
                .entries(List.of())
                .nextStreamPosition("0")
                .build();

        when(mockEventsManager.getEvents(any(GetEventsQueryParams.class))).thenReturn(emptyEvents);

        testRunner.run();

        testRunner.assertTransferCount(ConsumeBoxEvents.REL_SUCCESS, 0);
    }
}
