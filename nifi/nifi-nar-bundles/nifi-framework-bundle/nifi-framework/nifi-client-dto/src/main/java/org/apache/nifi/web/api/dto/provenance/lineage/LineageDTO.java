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
package org.apache.nifi.web.api.dto.provenance.lineage;

import java.util.Date;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimestampAdapter;

/**
 * Represents the lineage for a flowfile.
 */
@XmlType(name = "lineage")
public class LineageDTO {

    private String id;
    private String uri;
    private String clusterNodeId;

    private Date submissionTime;
    private Date expiration;
    private Integer percentCompleted;
    private Boolean finished;

    private LineageRequestDTO request;
    private LineageResultsDTO results;

    /**
     * The id of this lineage.
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
     * The uri for this lineage.
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
     * The id of the node in the cluster where this lineage originated.
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
     * The submission time for this lineage.
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
     * The expiration of this lineage.
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
     * Percent completed for this result.
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
     * Whether or not the request is finished running.
     *
     * @return
     */
    public Boolean getFinished() {
        return finished;
    }

    public void setFinished(Boolean finished) {
        this.finished = finished;
    }

    /**
     * The lineage request.
     *
     * @return
     */
    public LineageRequestDTO getRequest() {
        return request;
    }

    public void setRequest(LineageRequestDTO request) {
        this.request = request;
    }

    /**
     * The results of this lineage.
     *
     * @return
     */
    public LineageResultsDTO getResults() {
        return results;
    }

    public void setResults(LineageResultsDTO results) {
        this.results = results;
    }

}
