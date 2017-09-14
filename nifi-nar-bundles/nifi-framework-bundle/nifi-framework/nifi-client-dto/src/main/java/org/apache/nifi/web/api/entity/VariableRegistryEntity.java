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
import org.apache.nifi.web.api.dto.VariableRegistryDTO;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "variableRegistryEntity")
public class VariableRegistryEntity extends Entity {
    private RevisionDTO processGroupRevision;
    private VariableRegistryDTO variableRegistry;


    @ApiModelProperty("The Variable Registry.")
    public VariableRegistryDTO getVariableRegistry() {
        return variableRegistry;
    }

    public void setVariableRegistry(VariableRegistryDTO variableRegistry) {
        this.variableRegistry = variableRegistry;
    }

    @ApiModelProperty("The revision of the Process Group that the Variable Registry belongs to")
    public RevisionDTO getProcessGroupRevision() {
        return processGroupRevision;
    }

    public void setProcessGroupRevision(RevisionDTO processGroupRevision) {
        this.processGroupRevision = processGroupRevision;
    }
}
