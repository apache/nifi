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

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.util.DateTimeAdapter;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;
import java.util.Map;

/**
 * A request for provenance.
 */
@XmlType(name = "provenanceRequest")
public class ProvenanceRequestDTO {

    private Map<String, String> searchTerms;
    private String clusterNodeId;
    private Date startDate;
    private Date endDate;
    private String minimumFileSize;
    private String maximumFileSize;
    private Integer maxResults;

    private Boolean summarize;
    private Boolean incrementalResults;

    /**
     * @return the search terms to use for this search
     */
    @ApiModelProperty(
            value = "The search terms used to perform the search."
    )
    public Map<String, String> getSearchTerms() {
        return searchTerms;
    }

    public void setSearchTerms(final Map<String, String> searchTerms) {
        this.searchTerms = searchTerms;
    }

    /**
     * @return earliest event time to include in the query
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @ApiModelProperty(
            value = "The earliest event time to include in the query.",
            dataType = "string"
    )
    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    /**
     * @return latest event time to include in the query
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @ApiModelProperty(
            value = "The latest event time to include in the query.",
            dataType = "string"
    )
    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    /**
     * @return minimum file size to include in the query
     */
    @ApiModelProperty(
            value = "The minimum file size to include in the query."
    )
    public String getMinimumFileSize() {
        return minimumFileSize;
    }

    public void setMinimumFileSize(String minimumFileSize) {
        this.minimumFileSize = minimumFileSize;
    }

    /**
     * @return maximum file size to include in the query
     */
    @ApiModelProperty(
            value = "The maximum file size to include in the query."
    )
    public String getMaximumFileSize() {
        return maximumFileSize;
    }

    public void setMaximumFileSize(String maximumFileSize) {
        this.maximumFileSize = maximumFileSize;
    }

    /**
     * @return number of max results
     */
    @ApiModelProperty(
            value = "The maximum number of results to include."
    )
    public Integer getMaxResults() {
        return maxResults;
    }

    public void setMaxResults(Integer maxResults) {
        this.maxResults = maxResults;
    }

    /**
     * @return id of the node in the cluster where this provenance originated
     */
    @ApiModelProperty(
            value = "The id of the node in the cluster where this provenance originated."
    )
    public String getClusterNodeId() {
        return clusterNodeId;
    }

    public void setClusterNodeId(String clusterNodeId) {
        this.clusterNodeId = clusterNodeId;
    }

    /**
     * @return whether or not incremental results are returned. If false, provenance events
     * are only returned once the query completes. This property is true by default.
     */
    @ApiModelProperty(
            value = "Whether or not incremental results are returned. If false, provenance events"
                    + " are only returned once the query completes. This property is true by default."
    )
    public Boolean getIncrementalResults() {
        return incrementalResults;
    }

    public void setIncrementalResults(Boolean incrementalResults) {
        this.incrementalResults = incrementalResults;
    }

    /**
     * @return whether or not to summarize provenance events returned. This property is false by default.
     */
    @ApiModelProperty(
            value = "Whether or not to summarize provenance events returned. This property is false by default."
    )
    public Boolean getSummarize() {
        return summarize;
    }

    public void setSummarize(Boolean summarize) {
        this.summarize = summarize;
    }
}
