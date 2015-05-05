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
package org.apache.nifi.web.api.dto.action.details;

import com.wordnik.swagger.annotations.ApiModelProperty;
import javax.xml.bind.annotation.XmlType;

/**
 * Details for connect Actions.
 */
@XmlType(name = "connectDetails")
public class ConnectDetailsDTO extends ActionDetailsDTO {

    private String sourceId;
    private String sourceName;
    private String sourceType;
    private String relationship;
    private String destinationId;
    private String destinationName;
    private String destinationType;

    /**
     * @return id of the source of the connection
     */
    @ApiModelProperty(
            value = "The id of the source of the connection."
    )
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * @return name of the source of the connection
     */
    @ApiModelProperty(
            value = "The name of the source of the connection."
    )
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    /**
     * @return type of the source of the connection
     */
    @ApiModelProperty(
            value = "The type of the source of the connection."
    )
    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    /**
     * @return name of the relationship that was connected
     */
    @ApiModelProperty(
            value = "The name of the relationship that was connected."
    )
    public String getRelationship() {
        return relationship;
    }

    public void setRelationship(String relationship) {
        this.relationship = relationship;
    }

    /**
     * @return id of the destination of the connection
     */
    @ApiModelProperty(
            value = "The id of the destination of the connection."
    )
    public String getDestinationId() {
        return destinationId;
    }

    public void setDestinationId(String destinationId) {
        this.destinationId = destinationId;
    }

    /**
     * @return name of the destination of the connection
     */
    @ApiModelProperty(
            value = "The name of the destination of the connection."
    )
    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    /**
     * @return type of the destination of the connection
     */
    @ApiModelProperty(
            value = "The type of the destination of the connection."
    )
    public String getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(String destinationType) {
        this.destinationType = destinationType;
    }

}
