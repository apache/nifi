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

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * A serialized representation of this class can be placed in the entity body of a response to the API. This particular entity holds a reference to an input PortDTO.
 */
@XmlRootElement(name = "portEntity")
public class PortEntity extends ComponentEntity implements Permissible<PortDTO>, OperationPermissible {

    private PortDTO component;
    private PortStatusDTO status;
    private String portType;
    private PermissionsDTO operatePermissions;

    /**
     * @return input PortDTO that are being serialized
     */
    @Override
    public PortDTO getComponent() {
        return component;
    }

    @Override
    public void setComponent(PortDTO component) {
        this.component = component;
    }

    /**
     * @return the port status
     */
    @ApiModelProperty(
        value = "The status of the port."
    )
    public PortStatusDTO getStatus() {
        return status;
    }

    public void setStatus(PortStatusDTO status) {
        this.status = status;
    }

    public String getPortType() {
        return portType;
    }

    public void setPortType(String portType) {
        this.portType = portType;
    }

    /**
     * @return The permissions for this component operations
     */
    @ApiModelProperty(
            value = "The permissions for this component operations."
    )
    @Override
    public PermissionsDTO getOperatePermissions() {
        return operatePermissions;
    }

    @Override
    public void setOperatePermissions(PermissionsDTO permissions) {
        this.operatePermissions = permissions;
    }
}
