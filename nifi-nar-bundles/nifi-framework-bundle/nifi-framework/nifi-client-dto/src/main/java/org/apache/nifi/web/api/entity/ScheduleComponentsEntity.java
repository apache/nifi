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

import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.nifi.web.api.dto.RevisionDTO;

import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * A serialized representation of this class can be placed in the entity body of a request to the API.
 */
@XmlRootElement(name = "scheduleComponentEntity")
public class ScheduleComponentsEntity extends Entity {
    public static final String STATE_RUNNING = "RUNNING";
    public static final String STATE_STOPPED = "STOPPED";

    private String id;
    private String state;
    private Map<String, RevisionDTO> components;

    /**
     * @return The id of the ProcessGroup
     */
    @ApiModelProperty(
        value = "The id of the ProcessGroup"
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return The desired state of the descendant components. Possible states are 'RUNNING' and 'STOPPED'
     */
    @ApiModelProperty(
        value = "The desired state of the descendant components",
        allowableValues = STATE_RUNNING + ", " + STATE_STOPPED
    )
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    /**
     * @return The components to schedule. If not specified, all authorized descendant components will be used.
     */
    @ApiModelProperty(
        value = "Optional components to schedule. If not specified, all authorized descendant components will be used."
    )
    public Map<String, RevisionDTO> getComponents() {
        return components;
    }

    public void setComponents(Map<String, RevisionDTO> components) {
        this.components = components;
    }
}
