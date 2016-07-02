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
import org.apache.nifi.web.api.dto.status.ProcessorStatusSnapshotDTO;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API. This particular entity holds a reference to a ProcessorStatusSnapshotDTO.
 */
public class ProcessorStatusSnapshotEntity extends Entity implements ReadablePermission {
    private String id;
    private ProcessorStatusSnapshotDTO processorStatusSnapshot;
    private Boolean canRead;

    /**
     * @return The processor id
     */
    @ApiModelProperty("The id of the processor.")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The ProcessorStatusSnapshotDTO that is being serialized.
     *
     * @return The ProcessorStatusSnapshotDTO object
     */
    public ProcessorStatusSnapshotDTO getProcessorStatusSnapshot() {
        return processorStatusSnapshot;
    }

    public void setProcessorStatusSnapshot(ProcessorStatusSnapshotDTO processorStatusSnapshot) {
        this.processorStatusSnapshot = processorStatusSnapshot;
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
    public ProcessorStatusSnapshotEntity clone() {
        final ProcessorStatusSnapshotEntity other = new ProcessorStatusSnapshotEntity();
        other.setCanRead(this.getCanRead());
        other.setProcessorStatusSnapshot(this.getProcessorStatusSnapshot().clone());

        return other;
    }
}
