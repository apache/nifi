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

package org.apache.nifi.registry.flow;

import java.util.List;
import java.util.Set;

import io.swagger.annotations.ApiModelProperty;

public class VersionedConnection extends VersionedComponent {
    private ConnectableComponent source;
    private ConnectableComponent destination;
    private Integer labelIndex;
    private Long zIndex;
    private Set<String> selectedRelationships;

    private Long backPressureObjectThreshold;
    private String backPressureDataSizeThreshold;
    private String flowFileExpiration;
    private List<String> prioritizers;
    private List<Position> bends;

    private String loadBalanceStrategy;
    private String partitioningAttribute;
    private String loadBalanceCompression;


    @ApiModelProperty("The source of the connection.")
    public ConnectableComponent getSource() {
        return source;
    }

    public void setSource(ConnectableComponent source) {
        this.source = source;
    }

    @ApiModelProperty("The destination of the connection.")
    public ConnectableComponent getDestination() {
        return destination;
    }

    public void setDestination(ConnectableComponent destination) {
        this.destination = destination;
    }

    @ApiModelProperty("The bend points on the connection.")
    public List<Position> getBends() {
        return bends;
    }

    public void setBends(List<Position> bends) {
        this.bends = bends;
    }

    @ApiModelProperty("The index of the bend point where to place the connection label.")
    public Integer getLabelIndex() {
        return labelIndex;
    }

    public void setLabelIndex(Integer labelIndex) {
        this.labelIndex = labelIndex;
    }

    @ApiModelProperty(
            value = "The z index of the connection.",
            name = "zIndex")  // Jackson maps this method name to JSON key "zIndex", but Swagger does not by default
    public Long getzIndex() {
        return zIndex;
    }

    public void setzIndex(Long zIndex) {
        this.zIndex = zIndex;
    }

    @ApiModelProperty("The selected relationship that comprise the connection.")
    public Set<String> getSelectedRelationships() {
        return selectedRelationships;
    }

    public void setSelectedRelationships(Set<String> relationships) {
        this.selectedRelationships = relationships;
    }


    @ApiModelProperty("The object count threshold for determining when back pressure is applied. Updating this value is a passive change in the sense that it won't impact whether existing files "
        + "over the limit are affected but it does help feeder processors to stop pushing too much into this work queue.")
    public Long getBackPressureObjectThreshold() {
        return backPressureObjectThreshold;
    }

    public void setBackPressureObjectThreshold(Long backPressureObjectThreshold) {
        this.backPressureObjectThreshold = backPressureObjectThreshold;
    }


    @ApiModelProperty("The object data size threshold for determining when back pressure is applied. Updating this value is a passive change in the sense that it won't impact whether existing "
        + "files over the limit are affected but it does help feeder processors to stop pushing too much into this work queue.")
    public String getBackPressureDataSizeThreshold() {
        return backPressureDataSizeThreshold;
    }

    public void setBackPressureDataSizeThreshold(String backPressureDataSizeThreshold) {
        this.backPressureDataSizeThreshold = backPressureDataSizeThreshold;
    }


    @ApiModelProperty("The amount of time a flow file may be in the flow before it will be automatically aged out of the flow. Once a flow file reaches this age it will be terminated from "
        + "the flow the next time a processor attempts to start work on it.")
    public String getFlowFileExpiration() {
        return flowFileExpiration;
    }

    public void setFlowFileExpiration(String flowFileExpiration) {
        this.flowFileExpiration = flowFileExpiration;
    }


    @ApiModelProperty("The comparators used to prioritize the queue.")
    public List<String> getPrioritizers() {
        return prioritizers;
    }

    public void setPrioritizers(List<String> prioritizers) {
        this.prioritizers = prioritizers;
    }

    @ApiModelProperty(value = "The Strategy to use for load balancing data across the cluster, or null, if no Load Balance Strategy has been specified.",
            allowableValues = "DO_NOT_LOAD_BALANCE, PARTITION_BY_ATTRIBUTE, ROUND_ROBIN, SINGLE_NODE")
    public String getLoadBalanceStrategy() {
        return loadBalanceStrategy;
    }

    public void setLoadBalanceStrategy(String loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }

    @ApiModelProperty("The attribute to use for partitioning data as it is load balanced across the cluster. If the Load Balance Strategy is configured to use PARTITION_BY_ATTRIBUTE, the value " +
            "returned by this method is the name of the FlowFile Attribute that will be used to determine which node in the cluster should receive a given FlowFile. If the Load Balance Strategy is " +
            "unset or is set to any other value, the Partitioning Attribute has no effect.")
    public String getPartitioningAttribute() {
        return partitioningAttribute;
    }

    public void setPartitioningAttribute(final String partitioningAttribute) {
        this.partitioningAttribute = partitioningAttribute;
    }

    @ApiModelProperty(value = "Whether or not compression should be used when transferring FlowFiles between nodes",
            allowableValues = "DO_NOT_COMPRESS, COMPRESS_ATTRIBUTES_ONLY, COMPRESS_ATTRIBUTES_AND_CONTENT")
    public String getLoadBalanceCompression() {
        return loadBalanceCompression;
    }

    public void setLoadBalanceCompression(final String compression) {
        this.loadBalanceCompression = compression;
    }

    @Override
    public ComponentType getComponentType() {
        return ComponentType.CONNECTION;
    }
}
