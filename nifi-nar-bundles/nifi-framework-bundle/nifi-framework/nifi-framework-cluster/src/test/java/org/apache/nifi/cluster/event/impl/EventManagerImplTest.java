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
package org.apache.nifi.cluster.event.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.cluster.event.Event;
import org.apache.nifi.cluster.event.Event.Category;
import org.apache.nifi.cluster.event.EventManager;
import org.junit.Test;

/**
 */
public class EventManagerImplTest {

    @Test(expected = IllegalArgumentException.class)
    public void testNonPositiveHistorySize() {
        new EventManagerImpl(0);
    }

    @Test
    public void testGetEventsUnknownSource() {
        EventManager manager = new EventManagerImpl(1);
        assertEquals(Collections.EMPTY_LIST, manager.getEvents("unknown value"));
    }

    @Test
    public void testGetEvents() {

        EventManager manager = new EventManagerImpl(2);

        Event e1 = new Event("1", "Event1", Category.INFO, 0);
        Event e2 = new Event("1", "Event2", Category.INFO, 1);

        manager.addEvent(e1);
        manager.addEvent(e2);

        List<Event> events = manager.getEvents("1");

        // assert newest to oldest
        assertEquals(Arrays.asList(e2, e1), events);
    }

    @Test
    public void testGetMostRecentEventUnknownSource() {
        EventManager manager = new EventManagerImpl(1);
        assertNull(manager.getMostRecentEvent("unknown value"));
    }

    @Test
    public void testGetMostRecentEvent() {

        EventManager manager = new EventManagerImpl(2);

        Event e1 = new Event("1", "Event1", Category.INFO, 0);
        Event e2 = new Event("1", "Event2", Category.INFO, 1);

        manager.addEvent(e1);
        manager.addEvent(e2);

        // assert newest to oldest
        assertEquals(e2, manager.getMostRecentEvent("1"));
    }

    @Test
    public void testAddEventExceedsHistorySize() {

        EventManager manager = new EventManagerImpl(1);

        Event e1 = new Event("1", "Event1", Category.INFO, 0);
        Event e2 = new Event("1", "Event2", Category.INFO, 1);

        manager.addEvent(e1);
        manager.addEvent(e2);

        List<Event> events = manager.getEvents("1");

        // assert oldest evicted
        assertEquals(Arrays.asList(e2), events);

    }

    @Test
    public void testClearHistory() {

        EventManager manager = new EventManagerImpl(1);

        Event e1 = new Event("1", "Event1", Category.INFO, 0);
        Event e2 = new Event("1", "Event2", Category.INFO, 1);

        manager.addEvent(e1);
        manager.addEvent(e2);

        manager.clearEventHistory("1");

        // assert oldest evicted
        assertTrue(manager.getEvents("1").isEmpty());

    }

}
