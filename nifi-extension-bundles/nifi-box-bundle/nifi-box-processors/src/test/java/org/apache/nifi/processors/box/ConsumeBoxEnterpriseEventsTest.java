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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConsumeBoxEnterpriseEventsTest extends AbstractBoxFileTest {

    private TestConsumeBoxEnterpriseEvents processor;

    @BeforeEach
    void setUp() throws Exception {
        processor = new TestConsumeBoxEnterpriseEvents();

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

        final TestEventStream eventStream = new TestEventStream();
        processor.overrideGetEventLog(eventStream::consume);

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

        assertEquals(eventIds.size(), testRunner.getCounterValue(ConsumeBoxEnterpriseEvents.COUNTER_RECORDS_PROCESSED));
    }

    static List<Arguments> dataFor_testConsumeEvents() {
        return List.of(
                arguments(StartEventPosition.EARLIEST, null, 2, List.of(0, 1, 2, 3)),
                arguments(StartEventPosition.OFFSET, "1", 2, List.of(1, 2, 3)),
                arguments(StartEventPosition.OFFSET, "12345", 1, List.of(3)),
                arguments(StartEventPosition.LATEST, null, 1, List.of(3))
        );
    }

    @Test
    void testGracefulTermination() throws InterruptedException {
        final CountDownLatch scheduledLatch = new CountDownLatch(1);
        final AtomicInteger consumedEvents = new AtomicInteger(0);

        // Infinite stream.
        processor.overrideGetEventLog(__ -> {
            scheduledLatch.countDown();
            consumedEvents.incrementAndGet();
            return createEventLog(List.of(createBoxEvent(1)), "");
        });

        final ExecutorService runExecutor = Executors.newSingleThreadExecutor();

        try {
            // Starting the processor that consumes an infinite stream.
            final Future<?> runFuture = runExecutor.submit(() -> testRunner.run(/*iterations=*/ 1, /*stopOnFinish=*/ false));

            assertTrue(scheduledLatch.await(5, TimeUnit.SECONDS), "Processor did not start");

            // Triggering the processor to stop.
            testRunner.unSchedule();

            assertDoesNotThrow(() -> runFuture.get(5, TimeUnit.SECONDS), "Processor did not stop gracefully");

            testRunner.assertAllFlowFilesTransferred(ConsumeBoxEnterpriseEvents.REL_SUCCESS, consumedEvents.get());
            assertEquals(consumedEvents.get(), testRunner.getCounterValue(ConsumeBoxEnterpriseEvents.COUNTER_RECORDS_PROCESSED));
        } finally {
            // We can't use try with resources, as Executors use a shutdown method
            // which indefinitely waits for submitted tasks.
            runExecutor.shutdownNow();
        }
    }

    private Stream<Integer> extractEventIds(final MockFlowFile flowFile) {
        final JsonValue json = Json.parse(flowFile.getContent());
        return json.asArray().values().stream()
                .map(JsonValue::asObject)
                .map(jsonObject -> jsonObject.get("id").asString())
                .map(Integer::parseInt);
    }

    /**
     * This class is used to override external call in {@link ConsumeBoxEnterpriseEvents#getEventLog(String)}.
     */
    private static class TestConsumeBoxEnterpriseEvents extends ConsumeBoxEnterpriseEvents {

        private volatile Function<String, EventLog> fakeEventLog;

        void overrideGetEventLog(final Function<String, EventLog> fakeEventLog) {
            this.fakeEventLog = fakeEventLog;
        }

        @Override
        EventLog getEventLog(String position) {
            return fakeEventLog.apply(position);
        }
    }

    private static class TestEventStream {

        private static final String NOW_POSITION = "now";

        private final List<BoxEvent> events = new ArrayList<>();

        void addEvent(final int eventId) {
            events.add(createBoxEvent(eventId));
        }

        EventLog consume(final String position) {
            final String nextPosition = String.valueOf(events.size());

            if (NOW_POSITION.equals(position)) {
                return createEventLog(emptyList(), nextPosition);
            }

            final int streamPosition = Integer.parseInt(position);
            if (streamPosition > events.size()) {
                // Real Box API returns the latest offset position, even if streamPosition was greater.
                return createEventLog(emptyList(), nextPosition);
            }

            final List<BoxEvent> consumedEvents = events.subList(streamPosition, events.size());

            return createEventLog(consumedEvents, nextPosition);
        }
    }

    private static BoxEvent createBoxEvent(final int eventId) {
        return new BoxEvent(null, "{\"event_id\": \"%d\"}".formatted(eventId));
    }

    private static EventLog createEventLog(final List<BoxEvent> consumedEvents, final String nextPosition) {
        // EventLog is not designed for being extended. Thus, mocking it.
        final EventLog eventLog = mock();

        when(eventLog.getNextStreamPosition()).thenReturn(nextPosition);
        lenient().when(eventLog.getSize()).thenReturn(consumedEvents.size());
        lenient().when(eventLog.iterator()).thenReturn(consumedEvents.iterator());

        return eventLog;
    }
}
