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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;


public class Query {

    private final String identifier;
    private final List<SearchTerm> searchTerms = new ArrayList<>();
    private Date startDate;
    private Date endDate;
    private String minFileSize;
    private String maxFileSize;
    private int maxResults = 1000;

    public Query(final String identifier) {
        this.identifier = Objects.requireNonNull(identifier);
    }

    public String getIdentifier() {
        return identifier;
    }

    public void addSearchTerm(final SearchTerm searchTerm) {
        searchTerms.add(searchTerm);
    }

    public List<SearchTerm> getSearchTerms() {
        return Collections.unmodifiableList(searchTerms);
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public int getMaxResults() {
        return maxResults;
    }

    public void setMaxResults(int maxResults) {
        this.maxResults = maxResults;
    }

    public void setMinFileSize(final String fileSize) {
        this.minFileSize = fileSize;
    }

    public String getMinFileSize() {
        return minFileSize;
    }

    public void setMaxFileSize(final String fileSize) {
        this.maxFileSize = fileSize;
    }

    public String getMaxFileSize() {
        return maxFileSize;
    }

    @Override
    public String toString() {
        return "Query[ " + searchTerms + " ]";
    }

    public boolean isEmpty() {
        return searchTerms.isEmpty() && maxFileSize == null && minFileSize == null && startDate == null && endDate == null;
    }
}
