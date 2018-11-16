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
import org.apache.nifi.web.api.dto.RevisionDTO;

import javax.xml.bind.annotation.XmlType;
import java.util.Arrays;

/**
 * Run status for a given component.
 */
@XmlType(name = "componentRunStatus")
public abstract class ComponentRunStatusEntity extends Entity {

    private RevisionDTO revision;
    private String state;
    private Boolean disconnectedNodeAcknowledged;

    /**
     * @return revision for this request/response
     */
    @ApiModelProperty(
            value = "The revision for this request/response. The revision is required for any mutable flow requests and is included in all responses."
    )
    public RevisionDTO getRevision() {
        return revision;
    }

    public void setRevision(RevisionDTO revision) {
        this.revision = revision;
    }
    /**
     * Run status for this component.
     * @return The run status
     */
    @ApiModelProperty(
            value = "The run status of the component."
    )
    public String getState() {
        return this.state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @ApiModelProperty(
            value = "Acknowledges that this node is disconnected to allow for mutable requests to proceed."
    )
    public Boolean isDisconnectedNodeAcknowledged() {
        return disconnectedNodeAcknowledged;
    }

    public void setDisconnectedNodeAcknowledged(Boolean disconnectedNodeAcknowledged) {
        this.disconnectedNodeAcknowledged = disconnectedNodeAcknowledged;
    }

    protected abstract String[] getSupportedState();

    public void validateState() {
        if (state == null || state.isEmpty()) {
            throw new IllegalArgumentException("The desired state is not set.");
        }

        if (Arrays.stream(getSupportedState()).noneMatch(state::equals)) {
            throw new IllegalArgumentException(String.format("The desired state '%s' is not supported.", state));
        }
    }

}
