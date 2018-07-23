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

import java.util.List;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlType;

import io.swagger.annotations.ApiModelProperty;

@XmlType(name = "garbageCollectionDiagnostics")
public class GarbageCollectionDiagnosticsDTO implements Cloneable {
    private String memoryManagerName;
    private List<GCDiagnosticsSnapshotDTO> snapshots;

    @ApiModelProperty("The name of the Memory Manager that this Garbage Collection information pertains to")
    public String getMemoryManagerName() {
        return memoryManagerName;
    }

    public void setMemoryManagerName(String memoryManagerName) {
        this.memoryManagerName = memoryManagerName;
    }

    @ApiModelProperty("A list of snapshots that have been taken to determine the health of the JVM's heap")
    public List<GCDiagnosticsSnapshotDTO> getSnapshots() {
        return snapshots;
    }

    public void setSnapshots(List<GCDiagnosticsSnapshotDTO> snapshots) {
        this.snapshots = snapshots;
    }

    @Override
    protected GarbageCollectionDiagnosticsDTO clone() {
        final GarbageCollectionDiagnosticsDTO clone = new GarbageCollectionDiagnosticsDTO();
        clone.memoryManagerName = memoryManagerName;
        if (snapshots != null) {
            clone.snapshots = snapshots.stream()
                .map(GCDiagnosticsSnapshotDTO::clone)
                .collect(Collectors.toList());
        }
        return clone;
    }
}
