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
package org.apache.nifi.provenance.search;

import java.util.Date;
import java.util.List;

import org.apache.nifi.provenance.ProvenanceEventRecord;

public interface QueryResult {

    /**
     * @return the Provenance events that match the query (up to the limit
     * specified in the query)
     */
    List<ProvenanceEventRecord> getMatchingEvents();

    /**
     * @return the total number of Provenance Events that hit
     */
    long getTotalHitCount();

    /**
     * @return the number of milliseconds the query took to run
     */
    long getQueryTime();

    /**
     * @return the date at which this QueryResult will expire
     */
    Date getExpiration();

    /**
     * @return If an error occurred while computing the lineage, this will return the
     * serialized error; otherwise, returns <code>null</code>
     */
    String getError();

    /**
     * @return an integer between 0 and 100 (inclusive) that indicates what
     * percentage of completion the query has reached
     */
    int getPercentComplete();

    /**
     * @return Indicates whether or not the query has finished running
     */
    boolean isFinished();
}
