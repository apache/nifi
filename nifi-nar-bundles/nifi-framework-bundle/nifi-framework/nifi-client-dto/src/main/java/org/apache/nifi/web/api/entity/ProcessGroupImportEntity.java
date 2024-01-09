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
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;

import jakarta.xml.bind.annotation.XmlRootElement;

/**
 * Entity for importing a process group that has been previously downloaded
 */
@XmlRootElement(name = "processGroupImportEntity")
public class ProcessGroupImportEntity extends ProcessGroupDescriptorEntity {
    private RegisteredFlowSnapshot versionedFlowSnapshot;

    @Schema(description = "The Versioned Flow Snapshot to import")
    public RegisteredFlowSnapshot getVersionedFlowSnapshot() {
        return versionedFlowSnapshot;
    }

    public void setVersionedFlowSnapshot(RegisteredFlowSnapshot versionedFlowSnapshot) {
        this.versionedFlowSnapshot = versionedFlowSnapshot;
    }
}
