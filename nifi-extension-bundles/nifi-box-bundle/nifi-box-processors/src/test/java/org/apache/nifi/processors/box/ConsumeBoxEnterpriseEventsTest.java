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

import com.box.sdk.BoxEvent;
import com.box.sdk.EventLog;
import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonValue;
import org.apache.nifi.processors.box.ConsumeBoxEnterpriseEvents.StartEventPosition;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumeBoxEnterpriseEventsTest extends AbstractBoxFileTest {

    private TestEventStream eventStream;

    @BeforeEach
    void setUp() throws Exception {
        eventStream = new TestEventStream();

        final ConsumeBoxEnterpriseEvents processor = new ConsumeBoxEnterpriseEvents() {
            @Override
            EventLog getEventLog(String position) {
                return eventStream.consume(position);
            }
        };

        testRunner = TestRunners.newTestRunner(processor);
        super.setUp();
    }

    @ParameterizedTest
    @MethodSource("dataFor_testConsumeEvents")
    void testConsumeEvents(
            final StartEventPosition startEventPosition,
            final @Nullable String startOffset,
            final int expectedFlowFiles,
            final List<Integer> expectedEventIds) {
        testRunner.setProperty(ConsumeBoxEnterpriseEvents.START_EVENT_POSITION, startEventPosition);
        if (startOffset != null) {
            testRunner.setProperty(ConsumeBoxEnterpriseEvents.START_OFFSET, startOffset);
        }

        eventStream.addEvent(0);
        eventStream.addEvent(1);
        eventStream.addEvent(2);
        testRunner.run();

        eventStream.addEvent(3);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(ConsumeBoxEnterpriseEvents.REL_SUCCESS, expectedFlowFiles);

        final List<Integer> eventIds = testRunner.getFlowFilesForRelationship(ConsumeBoxEnterpriseEvents.REL_SUCCESS).stream()
                .flatMap(this::extractEventIds)
                .toList();

        assertEquals(expectedEventIds, eventIds);
    }

    static List<Arguments> dataFor_testConsumeEvents() {
        return List.of(
                arguments(StartEventPosition.EARLIEST, null, 2, List.of(0, 1, 2, 3)),
                arguments(StartEventPosition.OFFSET, "1", 2, List.of(1, 2, 3)),
                arguments(StartEventPosition.OFFSET, "12345", 1, List.of(3)),
                arguments(StartEventPosition.LATEST, null, 1, List.of(3))
        );
    }

    private Stream<Integer> extractEventIds(final MockFlowFile flowFile) {
        final JsonValue json = Json.parse(flowFile.getContent());
        return json.asArray().values().stream()
                .map(JsonValue::asObject)
                .map(jsonObject -> jsonObject.get("id").asString())
                .map(Integer::parseInt);
    }

    private static class TestEventStream {

        private static final String NOW_POSITION = "now";

        private final List<BoxEvent> events = new ArrayList<>();

        void addEvent(final int eventId) {
            final BoxEvent boxEvent = new BoxEvent(null, "{\"event_id\": \"%d\"}".formatted(eventId));
            events.add(boxEvent);
        }

        EventLog consume(final String position) {
            if (NOW_POSITION.equals(position)) {
                return createEmptyEventLog();
            }

            final int streamPosition = Integer.parseInt(position);
            if (streamPosition > events.size()) {
                // Real Box API returns the latest offset position, even if streamPosition was greater.
                return createEmptyEventLog();
            }

            final List<BoxEvent> consumedEvents = events.subList(streamPosition, events.size());

            return createEventLog(consumedEvents);
        }

        private EventLog createEmptyEventLog() {
            return createEventLog(emptyList());
        }

        private EventLog createEventLog(final List<BoxEvent> consumedEvents) {
            // EventLog is not designed for being extended. Thus, mocking it.
            final EventLog eventLog = mock();

            when(eventLog.getNextStreamPosition()).thenReturn(String.valueOf(events.size()));
            lenient().when(eventLog.getSize()).thenReturn(consumedEvents.size());
            lenient().when(eventLog.iterator()).thenReturn(consumedEvents.iterator());

            return eventLog;
        }
    }
}
