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

import com.wordnik.swagger.annotations.ApiModelProperty;
import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlType;

/**
 * Represents the request for lineage for a flowfile.
 */
@XmlType(name = "lineageRequest")
public class LineageRequestDTO {

    /**
     * The type of this lineage request.
     */
    @XmlType(name = "lineageRequestType")
    @XmlEnum
    public enum LineageRequestType {
        PARENTS,
        CHILDREN,
        FLOWFILE;
    };

    private Long eventId;
    private LineageRequestType lineageRequestType;

    private String uuid;
    private String clusterNodeId;

    /**
     * @return event id that was used to generate this lineage
     */
    @ApiModelProperty(
            value = "The event id that was used to generate this lineage, if applicable. The event id is allowed for any type of lineageRequestType. If the lineageRequestType is FLOWFILE and the "
                    + "flowfile uuid is also included in the request, the event id will be ignored."
    )
    public Long getEventId() {
        return eventId;
    }

    public void setEventId(Long eventId) {
        this.eventId = eventId;
    }

    /**
     * @return type of lineage request. Either 'PARENTS', 'CHILDREN', or 'FLOWFILE'. PARENTS will return the lineage for the flowfiles that are parents of the specified event. CHILDREN will return the
     * lineage of for the flowfiles that are children of the specified event. FLOWFILE will return the lineage for the specified flowfile.
     */
    @ApiModelProperty(
            value = "The type of lineage request. PARENTS will return the lineage for the flowfiles that are parents of the specified event. CHILDREN will return the lineage "
                    + "for the flowfiles that are children of the specified event. FLOWFILE will return the lineage for the specified flowfile.",
            allowableValues = "PARENTS, CHILDREN, and FLOWFILE"
    )
    public LineageRequestType getLineageRequestType() {
        return lineageRequestType;
    }

    public void setLineageRequestType(LineageRequestType lineageRequestType) {
        this.lineageRequestType = lineageRequestType;
    }

    /**
     * @return id of the node in the cluster where this lineage originated
     */
    @ApiModelProperty(value = "The id of the node where this lineage originated if clustered.")
    public String getClusterNodeId() {
        return clusterNodeId;
    }

    public void setClusterNodeId(String clusterNodeId) {
        this.clusterNodeId = clusterNodeId;
    }

    /**
     * @return uuid that was used to generate this lineage
     */
    @ApiModelProperty(
            value = "The flowfile uuid that was used to generate the lineage. The flowfile uuid is only allowed when the lineageRequestType is FLOWFILE and will take precedence over event id."
    )
    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

}
