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
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.status.FlowAnalysisRuleStatusDTO;


/**
 * A serialized representation of this class can be placed in the entity body of a response to the API. This particular entity holds a reference to a flow analysis rule.
 */
@XmlRootElement(name = "flowAnalysisRuleEntity")
public class FlowAnalysisRuleEntity extends ComponentEntity implements Permissible<FlowAnalysisRuleDTO>, OperationPermissible {
    private FlowAnalysisRuleDTO component;
    private PermissionsDTO operatePermissions;
    private FlowAnalysisRuleStatusDTO status;

    /**
     * @return flow analysis rule that is being serialized
     */
    @Override
    public FlowAnalysisRuleDTO getComponent() {
        return component;
    }

    @Override
    public void setComponent(FlowAnalysisRuleDTO component) {
        this.component = component;
    }

    /**
     * @return The permissions for this component operations
     */
    @Schema(description = "The permissions for this component operations.")
    @Override
    public PermissionsDTO getOperatePermissions() {
        return operatePermissions;
    }

    @Override
    public void setOperatePermissions(PermissionsDTO permissions) {
        this.operatePermissions = permissions;
    }

    /**
     * @return The status for this FlowAnalysisRule
     */
    @Schema(description = "The status for this FlowAnalysisRule.", accessMode = Schema.AccessMode.READ_ONLY)
    public FlowAnalysisRuleStatusDTO getStatus() {
        return status;
    }

    public void setStatus(FlowAnalysisRuleStatusDTO status) {
        this.status = status;
    }
}
