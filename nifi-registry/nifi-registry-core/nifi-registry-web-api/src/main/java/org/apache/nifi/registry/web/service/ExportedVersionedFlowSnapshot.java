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
package org.apache.nifi.registry.web.service;

import io.swagger.annotations.ApiModel;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.nifi.registry.flow.VersionedFlowSnapshot;

/**
 * <p>
 * Represents a snapshot of a versioned flow and a filename for exporting the flow. A versioned flow may change many times
 * over the course of its life. This flow is saved to the registry with information such as its name, a description,
 * and each version of the flow.
 * </p>
 *
 * @see VersionedFlowSnapshot
 */
@ApiModel
public class ExportedVersionedFlowSnapshot {

    @Valid
    @NotNull
    private VersionedFlowSnapshot versionedFlowSnapshot;

    @Valid
    @NotNull
    private String filename;

    public ExportedVersionedFlowSnapshot(final VersionedFlowSnapshot versionedFlowSnapshot, final String filename) {
        this.versionedFlowSnapshot = versionedFlowSnapshot;
        this.filename = filename;
    }

    public void setVersionedFlowSnapshot(VersionedFlowSnapshot versionedFlowSnapshot) {
        this.versionedFlowSnapshot = versionedFlowSnapshot;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public VersionedFlowSnapshot getVersionedFlowSnapshot() {
        return versionedFlowSnapshot;
    }

    public String getFilename() {
        return filename;
    }
}
