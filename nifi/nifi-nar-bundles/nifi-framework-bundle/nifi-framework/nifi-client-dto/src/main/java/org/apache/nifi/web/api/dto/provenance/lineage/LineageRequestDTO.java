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

    /**
     * @return event id that was used to generate this lineage
     */
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
    public LineageRequestType getLineageRequestType() {
        return lineageRequestType;
    }

    public void setLineageRequestType(LineageRequestType lineageRequestType) {
        this.lineageRequestType = lineageRequestType;
    }

    /**
     * @return uuid that was used to generate this lineage
     */
    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

}
