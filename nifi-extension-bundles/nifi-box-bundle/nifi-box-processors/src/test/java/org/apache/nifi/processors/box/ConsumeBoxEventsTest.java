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
import com.box.sdkgen.serialization.json.EnumWrapper;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ConsumeBoxEventsTest extends AbstractBoxFileTest {

    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<>();

    @Override
    @BeforeEach
    void setUp() throws Exception {

        final ConsumeBoxEvents testSubject = new ConsumeBoxEvents() {
            @Override
            public void onScheduled(ProcessContext context) {
                // do nothing
            }
        };
        testSubject.events = queue;

        testRunner = TestRunners.newTestRunner(testSubject);
        super.setUp();
    }

    @Test
    void testCaptureEvents() {
        Event event1 = mock(Event.class);
        when(event1.getEventId()).thenReturn("1");
        when(event1.getEventType()).thenReturn(new EnumWrapper<>(EventEventTypeField.ITEM_CREATE));

        Event event2 = mock(Event.class);
        when(event2.getEventId()).thenReturn("2");
        when(event2.getEventType()).thenReturn(new EnumWrapper<>(EventEventTypeField.ITEM_TRASH));

        queue.add(event1);
        queue.add(event2);

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ConsumeBoxEvents.REL_SUCCESS, 1);
        final MockFlowFile ff0 = testRunner.getFlowFilesForRelationship(ConsumeBoxEvents.REL_SUCCESS).getFirst();
        ff0.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        ff0.assertAttributeEquals("record.count", "2");

        final String content = ff0.getContent();
        assertTrue(content.contains("\"id\":\"1\""));
        assertTrue(content.contains("\"id\":\"2\""));
    }

}
