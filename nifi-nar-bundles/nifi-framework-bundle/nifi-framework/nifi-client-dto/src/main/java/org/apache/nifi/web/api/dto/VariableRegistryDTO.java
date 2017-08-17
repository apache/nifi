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
import org.apache.nifi.web.api.entity.VariableEntity;

import javax.xml.bind.annotation.XmlType;
import java.util.Set;

@XmlType(name = "variableRegistry")
public class VariableRegistryDTO {
    private Set<VariableEntity> variables;
    private String processGroupId;

    public void setVariables(final Set<VariableEntity> variables) {
        this.variables = variables;
    }

    @ApiModelProperty("The variables that are available in this Variable Registry")
    public Set<VariableEntity> getVariables() {
        return variables;
    }

    public void setProcessGroupId(final String processGroupId) {
        this.processGroupId = processGroupId;
    }

    @ApiModelProperty("The UUID of the Process Group that this Variable Registry belongs to")
    public String getProcessGroupId() {
        return processGroupId;
    }
}
