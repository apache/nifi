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

@XmlType(name = "parameterContextUpdateRequest")
public class ParameterContextUpdateRequestDTO extends AsynchronousRequestDTO<ParameterContextUpdateStepDTO> {
    private ParameterContextDTO parameterContext;
    private Set<AffectedComponentEntity> referencingComponents;

    @ApiModelProperty(value = "The Parameter Context that is being operated on. This may not be populated until the request has successfully completed.",
            accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public ParameterContextDTO getParameterContext() {
        return parameterContext;
    }

    public void setParameterContext(final ParameterContextDTO parameterContext) {
        this.parameterContext = parameterContext;
    }

    @ApiModelProperty(value = "The components that are referenced by the update.", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public Set<AffectedComponentEntity> getReferencingComponents() {
        return referencingComponents;
    }

    public void setReferencingComponents(final Set<AffectedComponentEntity> referencingComponents) {
        this.referencingComponents = referencingComponents;
    }
}
