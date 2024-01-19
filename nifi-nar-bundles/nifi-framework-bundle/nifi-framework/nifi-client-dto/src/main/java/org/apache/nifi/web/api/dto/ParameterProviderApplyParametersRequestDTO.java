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

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.nifi.web.api.entity.AffectedComponentEntity;
import org.apache.nifi.web.api.entity.ParameterContextUpdateEntity;

import jakarta.xml.bind.annotation.XmlType;
import java.util.List;
import java.util.Set;

@XmlType(name = "parameterProviderApplyParametersRequest")
public class ParameterProviderApplyParametersRequestDTO extends AsynchronousRequestDTO<ParameterProviderApplyParametersUpdateStepDTO> {
    private ParameterProviderDTO parameterProvider;
    private List<ParameterContextUpdateEntity> parameterContextUpdates;
    private Set<AffectedComponentEntity> referencingComponents;

    @Schema(description = "The Parameter Contexts updated by this Parameter Provider. This may not be populated until the request has successfully completed.",
            accessMode = Schema.AccessMode.READ_ONLY)
    public List<ParameterContextUpdateEntity> getParameterContextUpdates() {
        return parameterContextUpdates;
    }

    public void setParameterContextUpdates(final List<ParameterContextUpdateEntity> parameterContextUpdates) {
        this.parameterContextUpdates = parameterContextUpdates;
    }

    @Schema(description = "The Parameter Provider that is being operated on. This may not be populated until the request has successfully completed.",
            accessMode = Schema.AccessMode.READ_ONLY)
    public ParameterProviderDTO getParameterProvider() {
        return parameterProvider;
    }

    public void setParameterProvider(final ParameterProviderDTO parameterProvider) {
        this.parameterProvider = parameterProvider;
    }

    @Schema(description = "The components that are referenced by the update.", accessMode = Schema.AccessMode.READ_ONLY)
    public Set<AffectedComponentEntity> getReferencingComponents() {
        return referencingComponents;
    }

    public void setReferencingComponents(final Set<AffectedComponentEntity> referencingComponents) {
        this.referencingComponents = referencingComponents;
    }
}
