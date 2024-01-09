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
import org.apache.nifi.web.api.entity.ParameterEntity;

import jakarta.xml.bind.annotation.XmlType;

@XmlType(name = "parameterStatus")
public class ParameterStatusDTO {
    private ParameterEntity parameter;
    private ParameterStatus status;

    @Schema(description = "The name of the Parameter")
    public ParameterEntity getParameter() {
        return parameter;
    }

    public void setParameter(final ParameterEntity parameter) {
        this.parameter = parameter;
    }

    @Schema(description = "Indicates the status of the parameter, compared to the existing parameter context")
    public ParameterStatus getStatus() {
        return status;
    }

    public void setStatus(ParameterStatus status) {
        this.status = status;
    }
}
