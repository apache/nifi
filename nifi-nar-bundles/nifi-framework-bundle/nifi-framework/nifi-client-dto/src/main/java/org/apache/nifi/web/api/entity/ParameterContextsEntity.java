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
import org.apache.nifi.web.api.dto.util.TimeAdapter;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;
import java.util.Set;

@XmlRootElement(name = "parameterContexts")
public class ParameterContextsEntity extends Entity {
    private Set<ParameterContextEntity> parameterContexts;
    private Date currentTime;

    @ApiModelProperty("The Parameter Contexts")
    public Set<ParameterContextEntity> getParameterContexts() {
        return parameterContexts;
    }

    public void setParameterContexts(final Set<ParameterContextEntity> parameterContexts) {
        this.parameterContexts = parameterContexts;
    }

    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
        value = "The current time on the system.",
        dataType = "string",
        accessMode = ApiModelProperty.AccessMode.READ_ONLY
    )
    public Date getCurrentTime() {
        return currentTime;
    }

    public void setCurrentTime(Date currentTime) {
        this.currentTime = currentTime;
    }
}
