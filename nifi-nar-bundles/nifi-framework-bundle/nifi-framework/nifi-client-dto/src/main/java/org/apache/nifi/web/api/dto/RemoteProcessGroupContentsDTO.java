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
package org.apache.nifi.web.api.dto;

import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.Set;
import javax.xml.bind.annotation.XmlType;

/**
 * Contents of a remote process group.
 */
@XmlType(name = "remoteProcessGroupContents")
public class RemoteProcessGroupContentsDTO {

    private Set<RemoteProcessGroupPortDTO> inputPorts;
    private Set<RemoteProcessGroupPortDTO> outputPorts;

    /**
     * @return Controller Input Ports to which data can be sent
     */
    @ApiModelProperty(
            value = "The input ports to which data can be sent."
    )
    public Set<RemoteProcessGroupPortDTO> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Set<RemoteProcessGroupPortDTO> inputPorts) {
        this.inputPorts = inputPorts;
    }

    /**
     * @return Controller Output Ports from which data can be retrieved
     */
    @ApiModelProperty(
            value = "The output ports from which data can be retrieved."
    )
    public Set<RemoteProcessGroupPortDTO> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Set<RemoteProcessGroupPortDTO> outputPorts) {
        this.outputPorts = outputPorts;
    }
}
