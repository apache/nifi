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

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;

import javax.xml.bind.annotation.XmlType;
import java.util.Set;

@XmlType(name = "variableRegistryUpdateRequest")
public class VariableRegistryUpdateRequestDTO extends AsynchronousRequestDTO<VariableRegistryUpdateStepDTO> {
    private String processGroupId;
    private Set<AffectedComponentEntity> affectedComponents;

    @ApiModelProperty("The unique ID of the Process Group that the variable registry belongs to")
    public String getProcessGroupId() {
        return processGroupId;
    }

    public void setProcessGroupId(String processGroupId) {
        this.processGroupId = processGroupId;
    }

    @ApiModelProperty(value = "A set of all components that will be affected if the value of this variable is changed",
            accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public Set<AffectedComponentEntity> getAffectedComponents() {
        return affectedComponents;
    }

    public void setAffectedComponents(Set<AffectedComponentEntity> affectedComponents) {
        this.affectedComponents = affectedComponents;
    }
}
