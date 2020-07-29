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
package org.apache.nifi.web.api.dto;

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;
import java.util.List;
import java.util.Set;

/**
 * A connection between two connectable components.
 */
@XmlType(name = "connection")
public class ConnectionDTO extends ComponentDTO {
    public static final String LOAD_BALANCE_NOT_CONFIGURED = "LOAD_BALANCE_NOT_CONFIGURED";
    public static final String LOAD_BALANCE_INACTIVE = "LOAD_BALANCE_INACTIVE";
    public static final String LOAD_BALANCE_ACTIVE = "LOAD_BALANCE_ACTIVE";

    private ConnectableDTO source;
    private ConnectableDTO destination;
    private String name;
    private Integer labelIndex;
    private Long zIndex;
    private Set<String> selectedRelationships;
    private Set<String> availableRelationships;

    private Long backPressureObjectThreshold;
    private String backPressureDataSizeThreshold;
    private String flowFileExpiration;
    private List<String> prioritizers;
    private List<PositionDTO> bends;

    private String loadBalanceStrategy;
    private String loadBalancePartitionAttribute;
    private String loadBalanceCompression;
    private String loadBalanceStatus;

    /**
     * The source of this connection.
     *
     * @return The source of this connection
     */
    @ApiModelProperty(
            value = "The source of the connection."
    )
    public ConnectableDTO getSource() {
        return source;
    }

    public void setSource(ConnectableDTO source) {
        this.source = source;
    }

    /**
     * The destination of this connection.
     *
     * @return The destination of this connection
     */
    @ApiModelProperty(
            value = "The destination of the connection."
    )
    public ConnectableDTO getDestination() {
        return destination;
    }

    public void setDestination(ConnectableDTO destination) {
        this.destination = destination;
    }

    /**
     * @return name of the connection
     */
    @ApiModelProperty(
            value = "The name of the connection."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
     * The relationships that make up this connection.
     *
     * @return The relationships
     */
    @ApiModelProperty(
            value = "The selected relationship that comprise the connection."
    )
    public Set<String> getSelectedRelationships() {
        return selectedRelationships;
    }

    public void setSelectedRelationships(Set<String> relationships) {
        this.selectedRelationships = relationships;
    }

    /**
     * @return relationships that the source of the connection currently supports. This property is read only
     */
    @ApiModelProperty(
            value = "The relationships that the source of the connection currently supports.",
            readOnly = true
    )
    public Set<String> getAvailableRelationships() {
        return availableRelationships;
    }

    public void setAvailableRelationships(Set<String> availableRelationships) {
        this.availableRelationships = availableRelationships;
    }

    /**
     * The object count threshold for determining when back pressure is applied. Updating this value is a passive change in the sense that it won't impact whether existing files over the limit are
     * affected but it does help feeder processors to stop pushing too much into this work queue.
     *
     * @return The back pressure object threshold
     */
    @ApiModelProperty(
            value = "The object count threshold for determining when back pressure is applied. Updating this value is a passive change in the sense that it won't impact whether existing files "
                    + "over the limit are affected but it does help feeder processors to stop pushing too much into this work queue."
    )
    public Long getBackPressureObjectThreshold() {
        return backPressureObjectThreshold;
    }

    public void setBackPressureObjectThreshold(Long backPressureObjectThreshold) {
        this.backPressureObjectThreshold = backPressureObjectThreshold;
    }

    /**
     * The object data size threshold for determining when back pressure is applied. Updating this value is a passive change in the sense that it won't impact whether existing files over the limit are
     * affected but it does help feeder processors to stop pushing too much into this work queue.
     *
     * @return The back pressure data size threshold
     */
    @ApiModelProperty(
            value = "The object data size threshold for determining when back pressure is applied. Updating this value is a passive change in the sense that it won't impact whether existing "
                    + "files over the limit are affected but it does help feeder processors to stop pushing too much into this work queue."
    )
    public String getBackPressureDataSizeThreshold() {
        return backPressureDataSizeThreshold;
    }

    public void setBackPressureDataSizeThreshold(String backPressureDataSizeThreshold) {
        this.backPressureDataSizeThreshold = backPressureDataSizeThreshold;
    }

    /**
     * The amount of time a flow file may be in the flow before it will be automatically aged out of the flow. Once a flow file reaches this age it will be terminated from the flow the next time a
     * processor attempts to start work on it.
     *
     * @return The flow file expiration in minutes
     */
    @ApiModelProperty(
            value = "The amount of time a flow file may be in the flow before it will be automatically aged out of the flow. Once a flow file reaches this age it will be terminated from "
                    + "the flow the next time a processor attempts to start work on it."
    )
    public String getFlowFileExpiration() {
        return flowFileExpiration;
    }

    public void setFlowFileExpiration(String flowFileExpiration) {
        this.flowFileExpiration = flowFileExpiration;
    }

    /**
     * The prioritizers this connection is using.
     *
     * @return The prioritizer list
     */
    @ApiModelProperty(
            value = "The comparators used to prioritize the queue."
    )
    public List<String> getPrioritizers() {
        return prioritizers;
    }

    public void setPrioritizers(List<String> prioritizers) {
        this.prioritizers = prioritizers;
    }

    @ApiModelProperty(value = "How to load balance the data in this Connection across the nodes in the cluster.",
        allowableValues = "DO_NOT_LOAD_BALANCE, PARTITION_BY_ATTRIBUTE, ROUND_ROBIN, SINGLE_NODE")
    public String getLoadBalanceStrategy() {
        return loadBalanceStrategy;
    }

    public void setLoadBalanceStrategy(String loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }

    @ApiModelProperty(value = "The FlowFile Attribute to use for determining which node a FlowFile will go to if the Load Balancing Strategy is set to PARTITION_BY_ATTRIBUTE")
    public String getLoadBalancePartitionAttribute() {
        return loadBalancePartitionAttribute;
    }

    public void setLoadBalancePartitionAttribute(String partitionAttribute) {
        this.loadBalancePartitionAttribute = partitionAttribute;
    }

    @ApiModelProperty(value = "Whether or not data should be compressed when being transferred between nodes in the cluster.",
        allowableValues = "DO_NOT_COMPRESS, COMPRESS_ATTRIBUTES_ONLY, COMPRESS_ATTRIBUTES_AND_CONTENT")
    public String getLoadBalanceCompression() {
        return loadBalanceCompression;
    }

    public void setLoadBalanceCompression(String compression) {
        this.loadBalanceCompression = compression;
    }

    @ApiModelProperty(value = "The current status of the Connection's Load Balancing Activities. Status can indicate that Load Balancing is not configured for the connection, that Load Balancing " +
        "is configured but inactive (not currently transferring data to another node), or that Load Balancing is configured and actively transferring data to another node.",
        allowableValues = LOAD_BALANCE_NOT_CONFIGURED + ", " + LOAD_BALANCE_INACTIVE + ", " + LOAD_BALANCE_ACTIVE,
        readOnly = true)
    public String getLoadBalanceStatus() {
        return loadBalanceStatus;
    }

    public void setLoadBalanceStatus(String status) {
        this.loadBalanceStatus = status;
    }

    @Override
    public String toString() {
        return "ConnectionDTO [id: " + getId() + "]";
    }
}
