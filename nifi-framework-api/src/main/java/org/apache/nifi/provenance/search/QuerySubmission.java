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

public interface QuerySubmission {

    /**
     * @return the query that this submission pertains to
     */
    Query getQuery();

    /**
     * @return the {@link QueryResult} for this query. Note that the result is
     * only a partial result if the result of calling
     * {@link QueryResult#isFinished()} is <code>false</code>
     */
    QueryResult getResult();

    /**
     * @return the date at which this query was submitted
     */
    Date getSubmissionTime();

    /**
     * @return the generated identifier for this query result
     */
    String getQueryIdentifier();

    /**
     * Cancels the query
     */
    void cancel();

    /**
     * @return <code>true</code> if {@link #cancel()} has been called,
     * <code>false</code> otherwise
     */
    boolean isCanceled();

    /**
     * @return the identity of the user who submitted the query
     */
    String getSubmitterIdentity();
}
