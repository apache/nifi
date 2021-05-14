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
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.web.api.dto.ProcessGroupReplaceRequestDTO;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Entity for capturing the status of a Process Group replace request
 */
@XmlRootElement(name = "processGroupReplaceRequestEntity")
public class ProcessGroupReplaceRequestEntity extends FlowUpdateRequestEntity<ProcessGroupReplaceRequestDTO> {
    private VersionedFlowSnapshot versionedFlowSnapshot;

    @ApiModelProperty(value = "Returns the Versioned Flow to replace with", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public VersionedFlowSnapshot getVersionedFlowSnapshot() {
        return versionedFlowSnapshot;
    }

    public void setVersionedFlowSnapshot(VersionedFlowSnapshot versionedFlowSnapshot) {
        this.versionedFlowSnapshot = versionedFlowSnapshot;
    }

    @ApiModelProperty("The Process Group Change Request")
    @Override
    public ProcessGroupReplaceRequestDTO getRequest() {
        if (request == null) {
            request = new ProcessGroupReplaceRequestDTO();
        }
        return request;
    }

    public void setRequest(ProcessGroupReplaceRequestDTO request) {
        this.request = request;
    }

}
