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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.nifi.cluster.event.Event;
import org.apache.nifi.cluster.event.EventManager;

/**
 * Implements the EventManager.
 *
 * @author unattributed
 */
public class EventManagerImpl implements EventManager {

    /**
     * associates the source ID with an ordered queue of events, ordered by most recent event
     */
    private final Map<String, Queue<Event>> eventsMap = new HashMap<>();

    /**
     * the number of events to maintain for a given source
     */
    private final int eventHistorySize;

    /**
     * Creates an instance.
     *
     * @param eventHistorySize the number of events to manage for a given source. Value must be positive.
     */
    public EventManagerImpl(final int eventHistorySize) {
        if (eventHistorySize <= 0) {
            throw new IllegalArgumentException("Event history size must be positive: " + eventHistorySize);
        }
        this.eventHistorySize = eventHistorySize;
    }

    @Override
    public void addEvent(final Event event) {

        if (event == null) {
            throw new IllegalArgumentException("Event may not be null.");
        }

        Queue<Event> events = eventsMap.get(event.getSource());
        if (events == null) {
            // no events from this source, so add a new queue to the map
            events = new PriorityQueue<>(eventHistorySize, createEventComparator());
            eventsMap.put(event.getSource(), events);
        }

        // add event
        events.add(event);

        // if we exceeded the history size, then evict the oldest event
        if (events.size() > eventHistorySize) {
            removeOldestEvent(events);
        }

    }

    @Override
    public List<Event> getEvents(final String eventSource) {
        final Queue<Event> events = eventsMap.get(eventSource);
        if (events == null) {
            return Collections.EMPTY_LIST;
        } else {
            return Collections.unmodifiableList(new ArrayList<>(events));
        }
    }

    @Override
    public int getEventHistorySize() {
        return eventHistorySize;
    }

    @Override
    public Event getMostRecentEvent(final String eventSource) {
        final Queue<Event> events = eventsMap.get(eventSource);
        if (events == null) {
            return null;
        } else {
            return events.peek();
        }
    }

    @Override
    public void clearEventHistory(final String eventSource) {
        eventsMap.remove(eventSource);
    }

    private Comparator createEventComparator() {
        return new Comparator<Event>() {
            @Override
            public int compare(final Event o1, final Event o2) {
                // orders events by most recent first
                return (int) (o2.getTimestamp() - o1.getTimestamp());
            }
        };
    }

    private void removeOldestEvent(final Collection<Event> events) {

        if (events.isEmpty()) {
            return;
        }

        Event oldestEvent = null;
        for (final Event event : events) {
            if (oldestEvent == null || oldestEvent.getTimestamp() > event.getTimestamp()) {
                oldestEvent = event;
            }
        }

        events.remove(oldestEvent);

    }

}
