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
package org.apache.nifi.cluster.event;

import java.util.List;

/**
 * Manages an ordered list of events. The event history size dictates the total
 * number of events to manage for a given source at a given time. When the size
 * is exceeded, the oldest event for that source is evicted.
 *
 * @author unattributed
 */
public interface EventManager {

    /**
     * Adds an event to the manager.
     *
     * @param event an Event
     */
    void addEvent(Event event);

    /**
     * Returns a list of events for a given source sorted by the event's
     * timestamp where the most recent event is first in the list.
     *
     * @param eventSource the source
     *
     * @return the list of events
     */
    List<Event> getEvents(String eventSource);

    /*
     * Returns the most recent event for the source.  If no events exist, then
     * null is returned.
     */
    Event getMostRecentEvent(String eventSource);

    /*
     * Clears all events for the given source.
     */
    void clearEventHistory(String eventSource);

    /**
     * Returns the history size.
     *
     * @return the history size
     */
    int getEventHistorySize();

}
