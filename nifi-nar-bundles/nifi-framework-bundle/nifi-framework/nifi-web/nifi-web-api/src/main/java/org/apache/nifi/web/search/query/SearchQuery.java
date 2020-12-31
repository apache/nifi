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
package org.apache.nifi.web.search.query;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.groups.ProcessGroup;

/**
 * Represents the data set the search query executes based on.
 */
public interface SearchQuery {

    /**
     * The part of the query string not containing metadata (filters).
     *
     * @return The query string used for executing the search.
     */
    String getTerm();

    /**
     * Returns true if the query contains a given filter (regardless the value).
     *
     * @param filterName The name of the filter.
     *
     * @return True if the query contains the filter, false otherwise.
     */
    boolean hasFilter(String filterName);

    /**
     * Returns the value of the query filter. Should be checked with {@link #hasFilter} beforehand!
     *
     * @param filterName The name of the filter.
     *
     * @return The value of the filter if exists, otherwise it's null.
     */
    String getFilter(String filterName);

    /**
     * Returns the user who executes the query.
     *
     * @return Requesting user.
     */
    NiFiUser getUser();

    /**
     * References to the flow's root process group.
     *
     * @return Root group of the flow.
     */
    ProcessGroup getRootGroup();

    /**
     * References to the process group was active for the user during requesting the search. This might be the same as the root group.
     *
     * @return The user's active group.
     */
    ProcessGroup getActiveGroup();
}