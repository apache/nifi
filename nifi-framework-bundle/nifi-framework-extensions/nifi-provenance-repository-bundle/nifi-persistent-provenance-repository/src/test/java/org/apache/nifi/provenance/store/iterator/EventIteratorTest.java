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

package org.apache.nifi.provenance.store.iterator;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.TestUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventIteratorTest {

    @Test
    void testCanCreateAndRetrieveUsingOfFactory() throws IOException {
        final ProvenanceEventRecord event = TestUtil.createEvent();
        final EventIterator eventIterator = EventIterator.of(event);
        final Optional<ProvenanceEventRecord> foundEvent = eventIterator.nextEvent();
        assertTrue(foundEvent.isPresent());
        assertEquals(foundEvent.get().getAttribute("uuid"), event.getAttribute("uuid"));
    }

    @Test
    void testEmptyReturnedWhenExhausted() throws IOException {
        final EventIterator eventIterator = EventIterator.of(TestUtil.createEvent());
        assertTrue(eventIterator.nextEvent().isPresent());
        assertTrue(eventIterator.nextEvent().isEmpty());
    }

    @Test
    void testCanFilterEvents() throws IOException {
        final ProvenanceEventRecord eventOne = TestUtil.createEvent();
        final ProvenanceEventRecord eventTwo = TestUtil.createEvent();

        final EventIterator eventIterator = EventIterator.of(eventOne, eventTwo);
        // Filter out the first event
        final EventIterator filteredIterator  = eventIterator.filter((e) -> !Objects.equals(e.getAttribute("uuid"), eventOne.getAttribute("uuid")));

        final ProvenanceEventRecord foundEvent = filteredIterator.nextEvent().orElseThrow();
        assertEquals(foundEvent.getAttribute("uuid"), eventTwo.getAttribute("uuid"));
    }

    @Test
    void testEmptyFactoryIsEmpty() throws IOException {
        final EventIterator eventIterator = EventIterator.EMPTY;
        assertTrue(eventIterator.nextEvent().isEmpty());
    }
}
