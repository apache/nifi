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
import org.apache.nifi.web.api.dto.util.TimeAdapter;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Results of a provenance request.
 */
@XmlType(name = "provenanceResults")
public class ProvenanceResultsDTO {

    private List<ProvenanceEventDTO> provenanceEvents;
    private String total;
    private Long totalCount;
    private Date generated;
    private Date oldestEvent;
    private Integer timeOffset;

    private Set<String> errors;

    /**
     * @return error messages
     */
    @ApiModelProperty(
            value = "Any errors that occurred while performing the provenance request."
    )
    public Set<String> getErrors() {
        return errors;
    }

    public void setErrors(Set<String> errors) {
        this.errors = errors;
    }

    /**
     * @return provenance events that matched the search criteria
     */
    @ApiModelProperty(
            value = "The provenance events that matched the search criteria."
    )
    public List<ProvenanceEventDTO> getProvenanceEvents() {
        return provenanceEvents;
    }

    public void setProvenanceEvents(List<ProvenanceEventDTO> provenanceEvents) {
        this.provenanceEvents = provenanceEvents;
    }

    /**
     * @return total number of results formatted
     */
    @ApiModelProperty(
            value = "The total number of results formatted."
    )
    public String getTotal() {
        return total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    /**
     * @return total number of results
     */
    @ApiModelProperty(
            value = "The total number of results."
    )
    public Long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }

    /**
     * @return when the search was performed
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
            value = "Then the search was performed.",
            dataType = "string"
    )
    public Date getGenerated() {
        return generated;
    }

    public void setGenerated(Date generated) {
        this.generated = generated;
    }

    /**
     * @return oldest event available in the provenance repository
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @ApiModelProperty(
            value = "The oldest event available in the provenance repository.",
            dataType = "string"
    )
    public Date getOldestEvent() {
        return oldestEvent;
    }

    public void setOldestEvent(Date oldestEvent) {
        this.oldestEvent = oldestEvent;
    }

    /**
     * @return time offset on the server that's used for event time
     */
    @ApiModelProperty(
            value = "The time offset of the server that's used for event time."
    )
    public Integer getTimeOffset() {
        return timeOffset;
    }

    public void setTimeOffset(Integer timeOffset) {
        this.timeOffset = timeOffset;
    }

}
