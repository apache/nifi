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
import io.swagger.annotations.ApiModelProperty.AccessMode;

import javax.xml.bind.annotation.XmlType;
import java.util.List;
import java.util.Map;

@XmlType(name = "verifyConfigRequest")
public class VerifyConfigRequestDTO extends AsynchronousRequestDTO<VerifyConfigUpdateStepDTO> {
    private String componentId;
    private Map<String, String> properties;
    private Map<String, String> attributes;
    private List<ConfigVerificationResultDTO> results;

    @ApiModelProperty("The ID of the component whose configuration was verified")
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(final String componentId) {
        this.componentId = componentId;
    }

    @ApiModelProperty("The configured component properties")
    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(final Map<String, String> properties) {
        this.properties = properties;
    }

    @ApiModelProperty("FlowFile Attributes that should be used to evaluate Expression Language for resolving property values")
    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(final Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @ApiModelProperty(value="The Results of the verification", accessMode = AccessMode.READ_ONLY)
    public List<ConfigVerificationResultDTO> getResults() {
        return results;
    }

    public void setResults(final List<ConfigVerificationResultDTO> results) {
        this.results = results;
    }
}
