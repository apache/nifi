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

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.ReadablePermission;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusSnapshotDTO;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API. This particular entity holds a reference to a ProcessGroupStatusSnapshotDTO.
 */
public class ProcessGroupStatusSnapshotEntity extends Entity implements ReadablePermission, Cloneable {
    private String id;
    private ProcessGroupStatusSnapshotDTO processGroupStatusSnapshot;
    private Boolean canRead;

    /**
     * @return The process group id
     */
    @ApiModelProperty("The id of the process group.")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The ProcessGroupStatusSnapshotDTO that is being serialized.
     *
     * @return The ProcessGroupStatusSnapshotDTO object
     */
    public ProcessGroupStatusSnapshotDTO getProcessGroupStatusSnapshot() {
        return processGroupStatusSnapshot;
    }

    public void setProcessGroupStatusSnapshot(ProcessGroupStatusSnapshotDTO processGroupStatusSnapshot) {
        this.processGroupStatusSnapshot = processGroupStatusSnapshot;
    }

    @Override
    public Boolean getCanRead() {
        return canRead;
    }

    @Override
    public void setCanRead(Boolean canRead) {
        this.canRead = canRead;
    }

    @Override
    public ProcessGroupStatusSnapshotEntity clone() {
        final ProcessGroupStatusSnapshotEntity other = new ProcessGroupStatusSnapshotEntity();
        other.setCanRead(this.getCanRead());
        other.setProcessGroupStatusSnapshot(this.getProcessGroupStatusSnapshot().clone());

        return other;
    }
}
