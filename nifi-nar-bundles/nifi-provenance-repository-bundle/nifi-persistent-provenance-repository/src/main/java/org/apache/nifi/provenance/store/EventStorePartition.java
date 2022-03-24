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

package org.apache.nifi.provenance.store;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.store.iterator.EventIterator;

public interface EventStorePartition extends Closeable {
    /**
     * Performs any initialization routines that need to happen before the store is used
     *
     * @throws IOException if unable to perform initialization
     */
    void initialize() throws IOException;

    /**
     * Adds the given events to the store
     *
     * @param events the events to add
     * @return a mapping of event to the location where it was stored
     * @throws IOException if unable to add the events
     */
    StorageResult addEvents(Iterable<ProvenanceEventRecord> events) throws IOException;

    /**
     * @return the number of bytes occupied by the events in the store
     */
    long getSize();

    /**
     * @return the largest Event ID that has been written to this store, or -1 if no events have yet been stored.
     */
    long getMaxEventId();

    /**
     * Retrieves the event with the given ID
     *
     * @param id the ID of the event to retrieve
     * @return the Event with the given ID, or <code>null</code> if the event cannot be found
     * @throws IOException if unable to read the event from storage
     */
    Optional<ProvenanceEventRecord> getEvent(long id) throws IOException;

    /**
     * Retrieves up to maxRecords events from the store, starting with the event whose ID is equal to firstRecordId. If that
     * event cannot be found, then the first event will be the oldest event in the store whose ID is greater than firstRecordId.
     * All events will be returned in the order that they were written to the store. I.e., all events will have monotonically
     * increasing Event ID's.
     *
     * @param firstRecordId the ID of the first event to retrieve
     * @param maxEvents the maximum number of events to retrieve. The actual number of results returned may be less than this.
     * @param authorizer the authorizer that should be used to filter out any events that the user doesn't have access to
     * @return a List of ProvenanceEventRecord's
     * @throws IOException if unable to retrieve records from the store
     */
    List<ProvenanceEventRecord> getEvents(long firstRecordId, int maxEvents, EventAuthorizer authorizer) throws IOException;

    /**
     * Returns an {@link EventIterator} that is capable of iterating over the events in the store beginning with the given
     * record id. The events returned by the EventIterator will be provided in the order in which they were stored in the
     * partition. All events retrieved from this EventIterator will have monotonically increasing Event ID's.
     *
     * @param minimumEventId the minimum value of any Event ID that should be returned
     * @return an EventIterator that is capable of iterating over events in the store
     */
    EventIterator createEventIterator(long minimumEventId);

    /**
     * Returns an {@link EventIterator} that iterates over the given event ID's and returns one ProvenanceEventRecord for
     * each given, if the ID given can be found. If a given ID cannot be found, it will be skipped and no error will be reported.
     *
     * @param eventIds the ID's of the events to retrieve
     * @return an EventIterator that iterates over the given event ID's
     */
    EventIterator createEventIterator(List<Long> eventIds);

    /**
     * Purges any events from the partition that are older than the given amount of time
     *
     * @param olderThan the amount of time for which any event older than this should be removed
     * @param timeUnit the unit of time that applies to the first argument
     */
    void purgeOldEvents(long olderThan, TimeUnit timeUnit);

    /**
     * Purges some number of events from the partition. The oldest events will be purged.
     *
     * @return the number of bytes purged from the partition
     */
    long purgeOldestEvents();
}
