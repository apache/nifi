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
package org.apache.nifi.web.api.entity;

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * A serialized representation of this class can be placed in the entity body of a response to the API. This particular entity holds a reference to a ConnectionDTO.
 */
@XmlRootElement(name = "connectionEntity")
public class ConnectionEntity extends ComponentEntity implements Permissible<ConnectionDTO> {

    private ConnectionDTO component;
    private ConnectionStatusDTO status;
    private List<PositionDTO> bends;
    private Integer labelIndex;
    private Long zIndex;
    private String sourceId;
    private String sourceGroupId;
    private String sourceType;
    private String destinationId;
    private String destinationGroupId;
    private String destinationType;

    /**
     * @return RelationshipDTO that is being serialized
     */
    public ConnectionDTO getComponent() {
        return component;
    }

    public void setComponent(ConnectionDTO component) {
        this.component = component;
    }

    /**
     * @return the connection status
     */
    @ApiModelProperty(
        value = "The status of the connection."
    )
    public ConnectionStatusDTO getStatus() {
        return status;
    }

    public void setStatus(ConnectionStatusDTO status) {
        this.status = status;
    }

    /**
     * @return position of the bend points on this connection
     */
    @ApiModelProperty(
        value = "The bend points on the connection."
    )
    public List<PositionDTO> getBends() {
        return bends;
    }

    public void setBends(List<PositionDTO> bends) {
        this.bends = bends;
    }

    /**
     * @return The index of control point that the connection label should be placed over
     */
    @ApiModelProperty(
        value = "The index of the bend point where to place the connection label."
    )
    public Integer getLabelIndex() {
        return labelIndex;
    }

    public void setLabelIndex(Integer labelIndex) {
        this.labelIndex = labelIndex;
    }

    /**
     * @return z index for this connection
     */
    @ApiModelProperty(
            value = "The z index of the connection."
    )
    public Long getzIndex() {
        return zIndex;
    }

    public void setzIndex(Long zIndex) {
        this.zIndex = zIndex;
    }

    /**
     * @return The identifier of the source of this connection
     */
    @ApiModelProperty(
        value = "The identifier of the source of this connection."
    )
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * @return The identifier of the destination of this connection
     */
    @ApiModelProperty(
        value = "The identifier of the destination of this connection."
    )
    public String getDestinationId() {
        return destinationId;
    }

    public void setDestinationId(String destinationId) {
        this.destinationId = destinationId;
    }

    /**
     * @return The identifier of the group of the source of this connection
     */
    @ApiModelProperty(
        value = "The identifier of the group of the source of this connection."
    )
    public String getSourceGroupId() {
        return sourceGroupId;
    }

    public void setSourceGroupId(String sourceGroupId) {
        this.sourceGroupId = sourceGroupId;
    }

    /**
     * @return type of this source connectable component
     */
    @ApiModelProperty(
            value = "The type of component the source connectable is.",
            required = true,
            allowableValues = "PROCESSOR, REMOTE_INPUT_PORT, REMOTE_OUTPUT_PORT, INPUT_PORT, OUTPUT_PORT, FUNNEL"
    )
    public String getSourceType() {
        return sourceType;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
    }

    /**
     * @return The identifier of the group of the destination of this connection
     */
    @ApiModelProperty(
        value = "The identifier of the group of the destination of this connection."
    )
    public String getDestinationGroupId() {
        return destinationGroupId;
    }

    public void setDestinationGroupId(String destinationGroupId) {
        this.destinationGroupId = destinationGroupId;
    }

    /**
     * @return type of this destination connectable component
     */
    @ApiModelProperty(
            value = "The type of component the destination connectable is.",
            required = true,
            allowableValues = "PROCESSOR, REMOTE_INPUT_PORT, REMOTE_OUTPUT_PORT, INPUT_PORT, OUTPUT_PORT, FUNNEL"
    )
    public String getDestinationType() {
        return destinationType;
    }

    public void setDestinationType(String destinationType) {
        this.destinationType = destinationType;
    }
}
