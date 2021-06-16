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

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.api.dto.WritablePermission;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "parameterEntity")
public class ParameterEntity extends Entity implements WritablePermission {
    private Boolean canWrite;
    private ParameterDTO parameter;

    @ApiModelProperty("The parameter information")
    public ParameterDTO getParameter() {
        return parameter;
    }

    public void setParameter(final ParameterDTO parameter) {
        this.parameter = parameter;
    }

    @Override
    @ApiModelProperty(value = "Indicates whether the user can write a given resource.", readOnly = true)
    public Boolean getCanWrite() {
        return canWrite;
    }

    @Override
    public void setCanWrite(final Boolean canWrite) {
        this.canWrite = canWrite;
    }
}
