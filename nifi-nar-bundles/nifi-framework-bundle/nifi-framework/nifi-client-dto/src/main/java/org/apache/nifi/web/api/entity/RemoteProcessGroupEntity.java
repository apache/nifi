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

import javax.xml.bind.annotation.XmlRootElement;

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;

/**
 * A serialized representation of this class can be placed in the entity body of a request or response to or from the API. This particular entity holds a reference to a RemoteProcessGroupDTO.
 */
@XmlRootElement(name = "remoteProcessGroupEntity")
public class RemoteProcessGroupEntity extends ComponentEntity implements Permissible<RemoteProcessGroupDTO> {

    private RemoteProcessGroupDTO component;
    private RemoteProcessGroupStatusDTO status;

    private Integer inputPortCount;
    private Integer outputPortCount;

    /**
     * The RemoteProcessGroupDTO that is being serialized.
     *
     * @return The RemoteProcessGroupDTO object
     */
    public RemoteProcessGroupDTO getComponent() {
        return component;
    }

    public void setComponent(RemoteProcessGroupDTO component) {
        this.component = component;
    }

    /**
     * @return the remote process group status
     */
    @ApiModelProperty(
        value = "The status of the remote process group."
    )
    public RemoteProcessGroupStatusDTO getStatus() {
        return status;
    }

    public void setStatus(RemoteProcessGroupStatusDTO status) {
        this.status = status;
    }

    /**
     * @return number of Remote Input Ports currently available in the remote NiFi instance
     */
    @ApiModelProperty(
        value = "The number of remote input ports currently available on the target."
    )
    public Integer getInputPortCount() {
        return inputPortCount;
    }

    public void setInputPortCount(Integer inputPortCount) {
        this.inputPortCount = inputPortCount;
    }

    /**
     * @return number of Remote Output Ports currently available in the remote NiFi instance
     */
    @ApiModelProperty(
        value = "The number of remote output ports currently available on the target."
    )
    public Integer getOutputPortCount() {
        return outputPortCount;
    }

    public void setOutputPortCount(Integer outputPortCount) {
        this.outputPortCount = outputPortCount;
    }
}
