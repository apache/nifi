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
package org.apache.nifi.provenance;

import java.io.IOException;
import java.util.List;

/**
 * This Repository houses Provenance Events. The repository is responsible for
 * managing the life-cycle of the events, providing access to the events that it
 * has stored, and providing query capabilities against the events.
 *
 */
public interface ProvenanceEventRepository {

    /**
     * Returns a {@link ProvenanceEventBuilder} that is capable of building
     * {@link ProvenanceEventRecord}s
     *
     * @return builder
     */
    ProvenanceEventBuilder eventBuilder();

    /**
     * Adds the given event to the repository and returns a new event for which
     * the event id has been populated. Depending on the implementation, the
     * returned event may or may not be the same event given
     *
     * @param event to register
     */
    void registerEvent(ProvenanceEventRecord event);

    /**
     * Adds the given events to the repository.
     *
     * <p>
     * This interface makes no assumptions about whether or not the registration
     * of the Collection are atomic. This detail is implementation-specific.
     * </p>
     *
     * @param events to register
     */
    void registerEvents(Iterable<ProvenanceEventRecord> events);

    /**
     * Returns a List of all <code>ProvenanceEventRecord</code>s in the
     * repository starting with the given ID. The first ID in the repository
     * will always be 0 or higher. This method performs no authorization of
     * the events.
     *
     * @param firstRecordId id of the first record to retrieve
     * @param maxRecords maximum number of records to retrieve
     * @return records
     * @throws java.io.IOException if error reading from repository
     */
    List<ProvenanceEventRecord> getEvents(long firstRecordId, final int maxRecords) throws IOException;


    /**
     * @return the largest ID of any event that is queryable in the repository.
     *         If no queryable events exists, returns null
     */
    Long getMaxEventId();

    /**
     * Retrieves the Provenance Event with the given ID. The event will be returned only
     * if the given user is authorized to access the event. Otherwise, an
     * AccessDeniedException or ResourceNotFoundException will be thrown, as appropriate
     *
     * @param id to lookup
     * @return the Provenance Event Record with the given ID, if it exists, or
     *         {@code null} otherwise
     * @throws IOException if failure while retrieving event
     */
    ProvenanceEventRecord getEvent(long id) throws IOException;

    /**
     * Closes the repository, freeing any resources
     *
     * @throws IOException if failure closing repository
     */
    void close() throws IOException;


}
