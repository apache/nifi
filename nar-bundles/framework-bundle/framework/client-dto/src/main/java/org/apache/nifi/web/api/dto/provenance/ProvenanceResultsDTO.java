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
     * Any error messages.
     *
     * @return
     */
    public Set<String> getErrors() {
        return errors;
    }

    public void setErrors(Set<String> errors) {
        this.errors = errors;
    }

    /**
     * The provenance events that matched the search criteria.
     *
     * @return
     */
    public List<ProvenanceEventDTO> getProvenanceEvents() {
        return provenanceEvents;
    }

    public void setProvenanceEvents(List<ProvenanceEventDTO> provenanceEvents) {
        this.provenanceEvents = provenanceEvents;
    }

    /**
     * The total number of results formatted.
     *
     * @return
     */
    public String getTotal() {
        return total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    /**
     * The total number of results.
     *
     * @return
     */
    public Long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }

    /**
     * When the search was performed.
     *
     * @return
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    public Date getGenerated() {
        return generated;
    }

    public void setGenerated(Date generated) {
        this.generated = generated;
    }

    /**
     * The oldest event available in the provenance repository.
     *
     * @return
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    public Date getOldestEvent() {
        return oldestEvent;
    }

    public void setOldestEvent(Date oldestEvent) {
        this.oldestEvent = oldestEvent;
    }

    /**
     * The time offset on the server thats used for event time.
     *
     * @return
     */
    public Integer getTimeOffset() {
        return timeOffset;
    }

    public void setTimeOffset(Integer timeOffset) {
        this.timeOffset = timeOffset;
    }

}
