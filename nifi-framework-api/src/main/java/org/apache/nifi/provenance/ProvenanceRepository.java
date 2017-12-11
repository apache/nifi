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


import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public interface ProvenanceRepository extends ProvenanceEventRepository {

    /**
     * Performs any initialization needed. This should be called only by the
     * framework.
     *
     * @param eventReporter to report to
     * @param authorizer the authorizer to use for authorizing individual events
     * @param resourceFactory the resource factory to use for generating Provenance Resource objects for authorization purposes
     * @param identifierLookup a mechanism for looking up identifiers in the flow
     * @throws java.io.IOException if unable to initialize
     */
    void initialize(EventReporter eventReporter, Authorizer authorizer, ProvenanceAuthorizableFactory resourceFactory, IdentifierLookup identifierLookup) throws IOException;


    ProvenanceEventRecord getEvent(long id, NiFiUser user) throws IOException;

    /**
     * Returns a List of all <code>ProvenanceEventRecord</code>s in the
     * repository starting with the given ID. The first ID in the repository
     * will always be 0 or higher. Each event that is found will be authorized
     * against the given NiFiUser. If the user does not have authorization for
     * the event, the event will not be returned.
     *
     * @param firstRecordId id of the first record to retrieve
     * @param maxRecords    maximum number of records to retrieve
     * @param user          the NiFi user that the events should be authorized against
     * @return records
     * @throws java.io.IOException if error reading from repository
     */
    List<ProvenanceEventRecord> getEvents(long firstRecordId, final int maxRecords, NiFiUser user) throws IOException;

    /**
     * @return the {@link ProvenanceEventRepository} backing this ProvenanceRepository
     */
    ProvenanceEventRepository getProvenanceEventRepository();

    /**
     * Submits an asynchronous request to process the given query, returning an
     * identifier that can be used to fetch the results at a later time
     *
     * @param query to submit
     * @param user the NiFi User to authorize the events against
     *
     * @return an identifier that can be used to fetch the results at a later
     *         time
     */
    QuerySubmission submitQuery(Query query, NiFiUser user);

    /**
     * @param queryIdentifier of the query
     * @param user the user who is retrieving the query
     *
     * @return the QueryResult associated with the given identifier, if the
     *         query has finished processing. If the query has not yet finished running,
     *         returns <code>null</code>
     */
    QuerySubmission retrieveQuerySubmission(String queryIdentifier, NiFiUser user);

    /**
     * Submits a Lineage Computation to be completed and returns the
     * AsynchronousLineageResult that indicates the status of the request and
     * the results, if the computation is complete. If the given user does not
     * have authorization to view one of the events in the lineage, a placeholder
     * event will be used instead that provides none of the event details except
     * for the identifier of the component that emitted the Provenance Event. It is
     * necessary to include this node in the lineage view so that the lineage makes
     * sense, rather than showing disconnected graphs when the user is not authorized
     * for all components' provenance events.
     *
     * @param flowFileUuid the UUID of the FlowFile for which the Lineage should
     *            be calculated
     * @param user the NiFi User to authorize events against
     *
     * @return a {@link ComputeLineageSubmission} object that can be used to
     *         check if the computing is complete and if so get the results
     */
    ComputeLineageSubmission submitLineageComputation(String flowFileUuid, NiFiUser user);

    /**
     * Submits a Lineage Computation to be completed and returns the
     * AsynchronousLineageResult that indicates the status of the request and
     * the results, if the computation is complete. If the given user does not
     * have authorization to view one of the events in the lineage, a placeholder
     * event will be used instead that provides none of the event details except
     * for the identifier of the component that emitted the Provenance Event. It is
     * necessary to include this node in the lineage view so that the lineage makes
     * sense, rather than showing disconnected graphs when the user is not authorized
     * for all components' provenance events.
     *
     * This method is preferred to {@link #submitLineageComputation(String, NiFiUser)} because
     * it is much more efficient, but the former may be used if a particular Event ID is not
     * available.
     *
     * @param eventId the numeric ID of the event that the lineage is for
     * @param user the NiFi User to authorize events against
     *
     * @return a {@link ComputeLineageSubmission} object that can be used to
     *         check if the computing is complete and if so get the results
     */
    ComputeLineageSubmission submitLineageComputation(long eventId, NiFiUser user);

    /**
     * @param lineageIdentifier identifier of lineage to compute
     * @param user the user who is retrieving the lineage submission
     *
     * @return the {@link ComputeLineageSubmission} associated with the given
     *         identifier
     */
    ComputeLineageSubmission retrieveLineageSubmission(String lineageIdentifier, NiFiUser user);

    /**
     * Submits a request to expand the parents of the event with the given id. If the given user
     * is not authorized to access any event, a placeholder will be used instead that contains only
     * the ID of the component that emitted the event.
     *
     * @param eventId the one-up id of the Event to expand
     * @param user the NiFi user to authorize events against
     * @return a submission which can be checked for status
     *
     * @throws IllegalArgumentException if the given identifier identifies a
     *             Provenance Event that has a Type that is not expandable or if the
     *             identifier cannot be found
     */
    ComputeLineageSubmission submitExpandParents(long eventId, NiFiUser user);

    /**
     * Submits a request to expand the children of the event with the given id. If the given user
     * is not authorized to access any event, a placeholder will be used instead that contains only
     * the ID of the component that emitted the event.
     *
     * @param eventId the one-up id of the Event
     * @param user the NiFi user to authorize events against
     *
     * @return a submission which can be checked for status
     *
     * @throws IllegalArgumentException if the given identifier identifies a
     *             Provenance Event that has a Type that is not expandable or if the
     *             identifier cannot be found
     */
    ComputeLineageSubmission submitExpandChildren(long eventId, NiFiUser user);

    /**
     * @return a list of all fields that can be searched via the
     * {@link ProvenanceRepository#submitQuery(Query, NiFiUser)} method
     */
    List<SearchableField> getSearchableFields();

    /**
     * @return a list of all FlowFile attributes that can be searched via the
     * {@link ProvenanceRepository#submitQuery(Query, NiFiUser)} method
     */
    List<SearchableField> getSearchableAttributes();

    /**
     * @return the names of all Containers that exist for this Provenance
     * Repository
     */
    Set<String> getContainerNames();

    /**
     * @param containerName name of container to check capacity on
     * @return the maximum number of bytes that can be stored in the storage
     * mechanism that backs the container with the given name
     * @throws java.io.IOException if unable to check capacity
     * @throws IllegalArgumentException if no container exists with the given
     * name
     */
    long getContainerCapacity(String containerName) throws IOException;

    /**
     * @param containerName to check space on
     * @return the number of bytes available to be used used by the storage
     * mechanism that backs the container with the given name
     * @throws java.io.IOException if unable to check space
     * @throws IllegalArgumentException if no container exists with the given
     * name
     */
    long getContainerUsableSpace(String containerName) throws IOException;
}
