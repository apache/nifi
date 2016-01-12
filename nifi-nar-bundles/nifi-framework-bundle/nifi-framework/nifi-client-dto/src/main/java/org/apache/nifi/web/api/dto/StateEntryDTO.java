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

import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * Mapping of state for a given scope.
 */
@XmlType(name = "stateMap")
public class StateEntryDTO {

    private String key;
    private String value;

    private String clusterNodeId;    // include when clustered and scope is local
    private String clusterNodeAddress; // include when clustered and scope is local

    /**
     * @return the key for this state
     */
    @ApiModelProperty(
        value = "The key for this state."
    )
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    /**
     * @return the value for this state
     */
    @ApiModelProperty(
        value = "The value for this state."
    )
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    /**
     * @return identifier of the node where this state originated
     */
    @ApiModelProperty(
        value = "The identifier for the node where the state originated."
    )
    public String getClusterNodeId() {
        return clusterNodeId;
    }

    public void setClusterNodeId(String clusterNodeId) {
        this.clusterNodeId = clusterNodeId;
    }

    /**
     * @return label to use to show which node this state originated from
     */
    @ApiModelProperty(
        value = "The label for the node where the state originated."
    )
    public String getClusterNodeAddress() {
        return clusterNodeAddress;
    }

    public void setClusterNodeAddress(String clusterNodeAddress) {
        this.clusterNodeAddress = clusterNodeAddress;
    }
}
