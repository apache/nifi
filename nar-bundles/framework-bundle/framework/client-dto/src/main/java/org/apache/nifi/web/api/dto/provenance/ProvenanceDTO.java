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
 * A provenance submission. Incorporates the request, its current status, and
 * the results.
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
     * The id of this provenance query.
     *
     * @return
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The URI for this query. Used for obtaining the requests at a later time.
     *
     * @return
     */
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * The id of the node in the cluster where this provenance originated.
     *
     * @return
     */
    public String getClusterNodeId() {
        return clusterNodeId;
    }

    public void setClusterNodeId(String clusterNodeId) {
        this.clusterNodeId = clusterNodeId;
    }

    /**
     * The time the query was submitted.
     *
     * @return
     */
    @XmlJavaTypeAdapter(TimestampAdapter.class)
    public Date getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(Date submissionTime) {
        this.submissionTime = submissionTime;
    }

    /**
     * The expiration time of the query results.
     *
     * @return
     */
    @XmlJavaTypeAdapter(TimestampAdapter.class)
    public Date getExpiration() {
        return expiration;
    }

    public void setExpiration(Date expiration) {
        this.expiration = expiration;
    }

    /**
     * The percent completed.
     *
     * @return
     */
    public Integer getPercentCompleted() {
        return percentCompleted;
    }

    public void setPercentCompleted(Integer percentCompleted) {
        this.percentCompleted = percentCompleted;
    }

    /**
     * Whether the query has finished.
     *
     * @return
     */
    public Boolean isFinished() {
        return finished;
    }

    public void setFinished(Boolean finished) {
        this.finished = finished;
    }

    /**
     * The provenance request.
     *
     * @return
     */
    public ProvenanceRequestDTO getRequest() {
        return request;
    }

    public void setRequest(ProvenanceRequestDTO request) {
        this.request = request;
    }

    /**
     * The results of this query.
     *
     * @return
     */
    public ProvenanceResultsDTO getResults() {
        return results;
    }

    public void setResults(ProvenanceResultsDTO results) {
        this.results = results;
    }

}
