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
package org.apache.nifi.web.api.dto.diagnostics;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

@XmlType(name = "nodeAutoSchedulingDiagnostics")
public class NodeAutoSchedulingDiagnosticsDTO {
    private String nodeId;
    private String address;
    private Integer apiPort;
    private AutoSchedulingDiagnosticsDTO snapshot;

    @Schema(description = "Node identifier")
    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(final String nodeId) {
        this.nodeId = nodeId;
    }

    @Schema(description = "Node API address")
    public String getAddress() {
        return address;
    }

    public void setAddress(final String address) {
        this.address = address;
    }

    @Schema(description = "Node API port")
    public Integer getApiPort() {
        return apiPort;
    }

    public void setApiPort(final Integer apiPort) {
        this.apiPort = apiPort;
    }

    @Schema(description = "Node-specific automatic scheduling details")
    public AutoSchedulingDiagnosticsDTO getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(final AutoSchedulingDiagnosticsDTO snapshot) {
        this.snapshot = snapshot;
    }
}
