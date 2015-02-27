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
package org.apache.nifi.provenance.journaling.query;

import java.io.Closeable;

import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;

public interface QueryManager extends Closeable {
    /**
     * Submits an asynchronous request to process the given query, returning an
     * identifier that can be used to fetch the results at a later time
     *
     * @param query
     * @return
     */
    QuerySubmission submitQuery(Query query);
    
    /**
     * Returns the QueryResult associated with the given identifier, if the
     * query has finished processing. If the query has not yet finished running,
     * returns <code>null</code>.
     *
     * @param queryIdentifier
     *
     * @return
     */
    QuerySubmission retrieveQuerySubmission(String queryIdentifier);
    
    /**
     * Returns the {@link ComputeLineageSubmission} associated with the given
     * identifier
     *
     * @param lineageIdentifier
     * @return
     */
    ComputeLineageSubmission retrieveLineageSubmission(String lineageIdentifier);
    
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
     * Submits a request to expand the parents of the event with the given id
     *
     * @param eventRepo the repository from which to obtain the event information
     * @param eventId the one-up id of the Event to expand
     * @return
     *
     * @throws IllegalArgumentException if the given identifier identifies a
     * Provenance Event that has a Type that is not expandable or if the
     * identifier cannot be found
     */
    ComputeLineageSubmission submitExpandParents(final ProvenanceEventRepository eventRepo, long eventId);

    /**
     * Submits a request to expand the children of the event with the given id
     *
     * @param eventRepo the repository from which to obtain the event information
     * @param eventId the one-up id of the Event
     * @return
     *
     * @throws IllegalArgumentException if the given identifier identifies a
     * Provenance Event that has a Type that is not expandable or if the
     * identifier cannot be found
     */
    ComputeLineageSubmission submitExpandChildren(final ProvenanceEventRepository eventRepo, long eventId);
}
