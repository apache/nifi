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

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;

/**
 * This Repository houses Provenance Events. The repository is responsible for
 * managing the life-cycle of the events, providing access to the events that it
 * has stored, and providing query capabilities against the events.
 *
 */
public interface ProvenanceEventRepository {

    /**
     * Performs any initialization needed. This should be called only by the
     * framework.
     *
     * @param eventReporter to report to
     * @throws java.io.IOException if unable to initialize
     */
    void initialize(EventReporter eventReporter) throws IOException;

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
     * will always be 0 or higher.
     *
     * @param firstRecordId id of the first record to retrieve
     * @param maxRecords maximum number of records to retrieve
     * @return records
     * @throws java.io.IOException if error reading from repository
     */
    List<ProvenanceEventRecord> getEvents(long firstRecordId, final int maxRecords) throws IOException;

    /**
     * @return the largest ID of any event that is queryable in the repository.
     * If no queryable events exists, returns null
     */
    Long getMaxEventId();

    /**
     * Submits an asynchronous request to process the given query, returning an
     * identifier that can be used to fetch the results at a later time
     *
     * @param query to submit
     * @return an identifier that can be used to fetch the results at a later
     * time
     */
    QuerySubmission submitQuery(Query query);

    /**
     * @param queryIdentifier of the query
     *
     * @return the QueryResult associated with the given identifier, if the
     * query has finished processing. If the query has not yet finished running,
     * returns <code>null</code>
     */
    QuerySubmission retrieveQuerySubmission(String queryIdentifier);

    /**
     * Submits a Lineage Computation to be completed and returns the
     * AsynchronousLineageResult that indicates the status of the request and
     * the results, if the computation is complete.
     *
     * @param flowFileUuid the UUID of the FlowFile for which the Lineage should
     * be calculated
     * @return a {@link ComputeLineageSubmission} object that can be used to
     * check if the computing is complete and if so get the results
     */
    ComputeLineageSubmission submitLineageComputation(String flowFileUuid);

    /**
     * @param lineageIdentifier identifier of lineage to compute
     * @return the {@link ComputeLineageSubmission} associated with the given
     * identifier
     */
    ComputeLineageSubmission retrieveLineageSubmission(String lineageIdentifier);

    /**
     * @param id to lookup
     * @return the Provenance Event Record with the given ID, if it exists, or
     * {@code null} otherwise
     * @throws IOException if failure while retrieving event
     */
    ProvenanceEventRecord getEvent(long id) throws IOException;

    /**
     * Submits a request to expand the parents of the event with the given id
     *
     * @param eventId the one-up id of the Event to expand
     * @return a submission which can be checked for status
     *
     * @throws IllegalArgumentException if the given identifier identifies a
     * Provenance Event that has a Type that is not expandable or if the
     * identifier cannot be found
     */
    ComputeLineageSubmission submitExpandParents(long eventId);

    /**
     * Submits a request to expand the children of the event with the given id
     *
     * @param eventId the one-up id of the Event
     * @return a submission which can be checked for status
     *
     * @throws IllegalArgumentException if the given identifier identifies a
     * Provenance Event that has a Type that is not expandable or if the
     * identifier cannot be found
     */
    ComputeLineageSubmission submitExpandChildren(long eventId);

    /**
     * Closes the repository, freeing any resources
     *
     * @throws IOException if failure closing repository
     */
    void close() throws IOException;

    /**
     * @return a list of all fields that can be searched via the
     * {@link #submitQuery(nifi.provenance.search.Query)} method
     */
    List<SearchableField> getSearchableFields();

    /**
     * @return a list of all FlowFile attributes that can be searched via the
     * {@link #submitQuery(nifi.provenance.search.Query)} method
     */
    List<SearchableField> getSearchableAttributes();
}
