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

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.authorization.EventTransformer;
import org.apache.nifi.provenance.index.EventIndex;
import org.apache.nifi.provenance.serialization.StorageSummary;

/**
 * <p>
 * An Event Store is responsible for storing Provenance Events and retrieving them at a later time.
 * </p>
 */
public interface EventStore extends Closeable {

    /**
     * Performs any initialization routines that need to happen before the store is used
     *
     * @throws IOException if unable to perform initialization
     */
    void initialize() throws IOException;

    /**
     * Adds the given events to the store. All events will be written to the same Storage Location.
     * I.e., all of the {@link StorageSummary} objects that are provided when calling the {@link StorageResult#getStorageLocations()}
     * method will return the same value for the {@link StorageSummary#getStorageLocation()}. Each one, however, will
     * have a different Event ID and potentially a different Block Index.
     *
     * @param events the events to add
     * @return a mapping of event to the location where it was stored
     * @throws IOException if unable to add the events
     */
    StorageResult addEvents(Iterable<ProvenanceEventRecord> events) throws IOException;

    /**
     * @return the number of bytes occupied by the events in the store
     * @throws IOException if unable to determine the size of the store
     */
    long getSize() throws IOException;

    /**
     * @return the largest Event ID that has been written to this store, or -1 if no events have yet been stored.
     */
    long getMaxEventId();

    /**
     * Retrieves the event with the given ID
     *
     * @param id the ID of the event to retrieve
     * @return an Optional containing the Event with the given ID, or an empty optional if the event cannot be found
     * @throws IOException if unable to read the event from storage
     */
    Optional<ProvenanceEventRecord> getEvent(long id) throws IOException;

    /**
     * Retrieves up to maxRecords events from the store, starting with the event whose ID is equal to firstRecordId. If that
     * event cannot be found, then the first event will be the oldest event in the store whose ID is greater than firstRecordId.
     * All events will be returned in the order that they were written to the store. I.e., all events will have monotonically
     * increasing Event ID's. No events will be filtered out, since there is no EventAuthorizer provided.
     *
     * @param firstRecordId the ID of the first event to retrieve
     * @param maxRecords the maximum number of records to retrieve. The actual number of results returned may be less than this.
     * @return a List of ProvenanceEventRecord's
     * @throws IOException if unable to retrieve records from the store
     */
    List<ProvenanceEventRecord> getEvents(long firstRecordId, int maxRecords) throws IOException;

    /**
     * Retrieves up to maxRecords events from the store, starting with the event whose ID is equal to firstRecordId. If that
     * event cannot be found, then the first event will be the oldest event in the store whose ID is greater than firstRecordId.
     * All events will be returned in the order that they were written to the store. I.e., all events will have monotonically
     * increasing Event ID's.
     *
     * @param firstRecordId the ID of the first event to retrieve
     * @param maxRecords the maximum number of records to retrieve. The actual number of results returned may be less than this.
     * @param authorizer the authorizer that should be used to filter out any events that the user doesn't have access to
     * @param unauthorizedTransformer the transformer to apply to unauthorized events
     * @return a List of ProvenanceEventRecord's
     * @throws IOException if unable to retrieve records from the store
     */
    List<ProvenanceEventRecord> getEvents(long firstRecordId, int maxRecords, EventAuthorizer authorizer, EventTransformer unauthorizedTransformer) throws IOException;

    /**
     * Given a List of Event ID's, returns a List of Provenance Events that contain the events that have those corresponding
     * Event ID's. If any events cannot be found, a warning will be logged but no Exception will be thrown.
     *
     * @param eventIds a Stream of Event ID's
     * @param authorizer the authorizer that should be used to filter out any events that the user doesn't have access to
     * @param unauthorizedTransformer the transformer to apply to unauthorized events
     * @return a List of events that correspond to the given ID's
     * @throws IOException if unable to retrieve records from the store
     */
    List<ProvenanceEventRecord> getEvents(List<Long> eventIds, EventAuthorizer authorizer, EventTransformer unauthorizedTransformer) throws IOException;

    /**
     * Causes the latest events in this store to be re-indexed by the given Event Index
     *
     * @param eventIndex the EventIndex to use for indexing events
     */
    void reindexLatestEvents(EventIndex eventIndex);
}
