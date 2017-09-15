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
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.VariableRegistryUpdateRequestDTO;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "variableRegistryUpdateRequestEntity")
public class VariableRegistryUpdateRequestEntity extends Entity {
    private VariableRegistryUpdateRequestDTO request;
    private RevisionDTO processGroupRevision;

    @ApiModelProperty("The revision for the Process Group that owns this variable registry.")
    public RevisionDTO getProcessGroupRevision() {
        return processGroupRevision;
    }

    public void setProcessGroupRevision(RevisionDTO revision) {
        this.processGroupRevision = revision;
    }

    @ApiModelProperty("The Variable Registry Update Request")
    public VariableRegistryUpdateRequestDTO getRequest() {
        return request;
    }

    public void setRequest(VariableRegistryUpdateRequestDTO request) {
        this.request = request;
    }
}
