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

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.web.api.dto.util.TimestampAdapter;

/**
 * A provenance submission. Incorporates the request, its current status, and the results.
 */
@XmlType(name = "provenance")
public class ProvenanceDTO {

    private String id;
    private String uri;
    private String clusterNodeId;

    private Date submissionTime;
    private Date expiration;

    private Integer percentCompleted;
    private Boolean finished;

    private ProvenanceRequestDTO request;
    private ProvenanceResultsDTO results;

    /**
     * @return id of this provenance query
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return URI for this query. Used for obtaining the requests at a later time
     */
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * @return id of the node in the cluster where this provenance originated
     */
    public String getClusterNodeId() {
        return clusterNodeId;
    }

    public void setClusterNodeId(String clusterNodeId) {
        this.clusterNodeId = clusterNodeId;
    }

    /**
     * @return time the query was submitted
     */
    @XmlJavaTypeAdapter(TimestampAdapter.class)
    public Date getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(Date submissionTime) {
        this.submissionTime = submissionTime;
    }

    /**
     * @return expiration time of the query results
     */
    @XmlJavaTypeAdapter(TimestampAdapter.class)
    public Date getExpiration() {
        return expiration;
    }

    public void setExpiration(Date expiration) {
        this.expiration = expiration;
    }

    /**
     * @return percent completed
     */
    public Integer getPercentCompleted() {
        return percentCompleted;
    }

    public void setPercentCompleted(Integer percentCompleted) {
        this.percentCompleted = percentCompleted;
    }

    /**
     * @return whether the query has finished
     */
    public Boolean isFinished() {
        return finished;
    }

    public void setFinished(Boolean finished) {
        this.finished = finished;
    }

    /**
     * @return provenance request
     */
    public ProvenanceRequestDTO getRequest() {
        return request;
    }

    public void setRequest(ProvenanceRequestDTO request) {
        this.request = request;
    }

    /**
     * @return results of this query
     */
    public ProvenanceResultsDTO getResults() {
        return results;
    }

    public void setResults(ProvenanceResultsDTO results) {
        this.results = results;
    }

}
