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

@XmlRootElement(name = "activateControllerServicesEntity")
public class ActivateControllerServicesEntity extends Entity {
    public static final String STATE_ENABLED = "ENABLED";
    public static final String STATE_DISABLED = "DISABLED";

    private String id;
    private String state;
    private Map<String, RevisionDTO> components;

    @ApiModelProperty("The id of the ProcessGroup")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return The desired state of the descendant components. Possible states are 'RUNNING' and 'STOPPED'
     */
    @ApiModelProperty(value = "The desired state of the descendant components",
        allowableValues = STATE_ENABLED + ", " + STATE_DISABLED)
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @ApiModelProperty("Optional services to schedule. If not specified, all authorized descendant controller services will be used.")
    public Map<String, RevisionDTO> getComponents() {
        return components;
    }

    public void setComponents(Map<String, RevisionDTO> components) {
        this.components = components;
    }
}
