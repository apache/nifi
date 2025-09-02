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

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlType;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.status.ConnectorStatusDTO;

@XmlRootElement(name = "connectorEntity")
@XmlType(name = "connectorEntity")
public class ConnectorEntity extends ComponentEntity implements Permissible<ConnectorDTO>, OperationPermissible {
    private ConnectorDTO component;
    private ConnectorStatusDTO status;
    private PermissionsDTO operatePermissions;

    @Override
    @Schema(description = "The Connector DTO")
    public ConnectorDTO getComponent() {
        return component;
    }

    @Override
    public void setComponent(final ConnectorDTO component) {
        this.component = component;
    }

    @Schema(description = "The permissions for this component operations.")
    @Override
    public PermissionsDTO getOperatePermissions() {
        return operatePermissions;
    }

    @Override
    public void setOperatePermissions(final PermissionsDTO operatePermissions) {
        this.operatePermissions = operatePermissions;
    }

    @Schema(description = "The status of the connector.")
    public ConnectorStatusDTO getStatus() {
        return status;
    }

    public void setStatus(final ConnectorStatusDTO status) {
        this.status = status;
    }
}


