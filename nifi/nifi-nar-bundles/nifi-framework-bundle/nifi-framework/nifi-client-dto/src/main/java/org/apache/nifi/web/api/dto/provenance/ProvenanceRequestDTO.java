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
package org.apache.nifi.web.api.dto.provenance;

import java.util.Date;
import java.util.Map;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.DateTimeAdapter;

/**
 * A request for provenance.
 */
@XmlType(name = "provenanceRequest")
public class ProvenanceRequestDTO {

    private Map<String, String> searchTerms;
    private Date startDate;
    private Date endDate;
    private String minimumFileSize;
    private String maximumFileSize;
    private Integer maxResults;

    /**
     * Returns the search terms to use for this search
     *
     * @return
     */
    public Map<String, String> getSearchTerms() {
        return searchTerms;
    }

    public void setSearchTerms(final Map<String, String> searchTerms) {
        this.searchTerms = searchTerms;
    }

    /**
     * The earliest event time to include in the query
     *
     * @return
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    /**
     * The latest event time to include in the query
     *
     * @return
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    /**
     * The minimum file size to include in the query.
     *
     * @return
     */
    public String getMinimumFileSize() {
        return minimumFileSize;
    }

    public void setMinimumFileSize(String minimumFileSize) {
        this.minimumFileSize = minimumFileSize;
    }

    /**
     * The maximum file size to include in the query.
     *
     * @return
     */
    public String getMaximumFileSize() {
        return maximumFileSize;
    }

    public void setMaximumFileSize(String maximumFileSize) {
        this.maximumFileSize = maximumFileSize;
    }

    /**
     * The number of max results.
     *
     * @return
     */
    public Integer getMaxResults() {
        return maxResults;
    }

    public void setMaxResults(Integer maxResults) {
        this.maxResults = maxResults;
    }
}
