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

import javax.xml.bind.annotation.XmlType;
import java.util.Collection;

@XmlType(name = "componentValidationResult")
public class ComponentValidationResultDTO extends AffectedComponentDTO {
    private Boolean currentlyValid;
    private Boolean resultsValid;
    private Collection<String> resultantValidationErrors;

    @ApiModelProperty("Whether or not the component is currently valid")
    public Boolean getCurrentlyValid() {
        return currentlyValid;
    }

    public void setCurrentlyValid(final Boolean currentlyValid) {
        this.currentlyValid = currentlyValid;
    }

    @ApiModelProperty("Whether or not the component will be valid if the Parameter Context is changed")
    public Boolean getResultsValid() {
        return resultsValid;
    }

    public void setResultsValid(final Boolean resultsValid) {
        this.resultsValid = resultsValid;
    }

    @ApiModelProperty("The validation errors that will apply to the component if the Parameter Context is changed")
    public Collection<String> getResultantValidationErrors() {
        return resultantValidationErrors;
    }

    public void setResultantValidationErrors(final Collection<String> resultantValidationErrors) {
        this.resultantValidationErrors = resultantValidationErrors;
    }
}
