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
import java.util.List;

/**
 * Mapping of state for a given scope.
 */
@XmlType(name = "stateMap")
public class StateMapDTO {

    private String scope;
    private int totalEntryCount;
    private List<StateEntryDTO> state;

    /**
     * @return The scope of this StateMap
     */
    @ApiModelProperty(
        value = "The scope of this StateMap."
    )
    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    /**
     * @return The total number of state entries. When the state map is lengthy, only of portion of the entries are returned.
     */
    @ApiModelProperty(
        value = "The total number of state entries. When the state map is lengthy, only of portion of the entries are returned."
    )
    public int getTotalEntryCount() {
        return totalEntryCount;
    }

    public void setTotalEntryCount(int totalEntryCount) {
        this.totalEntryCount = totalEntryCount;
    }

    /**
     * @return The state
     */
    @ApiModelProperty(
        value = "The state."
    )
    public List<StateEntryDTO> getState() {
        return state;
    }

    public void setState(List<StateEntryDTO> state) {
        this.state = state;
    }

}
