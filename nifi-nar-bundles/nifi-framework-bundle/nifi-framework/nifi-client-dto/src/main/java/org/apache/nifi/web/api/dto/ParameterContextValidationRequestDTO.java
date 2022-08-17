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
import org.apache.nifi.web.api.entity.ComponentValidationResultsEntity;

import javax.xml.bind.annotation.XmlType;

@XmlType(name = "parameterContextValidationRequest")
public class ParameterContextValidationRequestDTO extends AsynchronousRequestDTO<ParameterContextValidationStepDTO> {
    private ParameterContextDTO parameterContext;
    private ComponentValidationResultsEntity componentValidationResults;

    @ApiModelProperty(value = "The Validation Results that were calculated for each component. This value may not be set until the request completes.",
            accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public ComponentValidationResultsEntity getComponentValidationResults() {
        return componentValidationResults;
    }

    public void setComponentValidationResults(final ComponentValidationResultsEntity componentValidationResults) {
        this.componentValidationResults = componentValidationResults;
    }

    @ApiModelProperty("The Parameter Context that is being operated on.")
    public ParameterContextDTO getParameterContext() {
        return parameterContext;
    }

    public void setParameterContext(final ParameterContextDTO parameterContext) {
        this.parameterContext = parameterContext;
    }

}
