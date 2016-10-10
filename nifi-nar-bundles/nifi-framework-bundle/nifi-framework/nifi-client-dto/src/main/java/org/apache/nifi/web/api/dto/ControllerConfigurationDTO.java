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

import javax.xml.bind.annotation.XmlType;

/**
 * Details for the controller configuration.
 */
@XmlType(name = "controllerConfiguration")
public class ControllerConfigurationDTO {

    private Integer maxTimerDrivenThreadCount;
    private Integer maxEventDrivenThreadCount;

    /**
     * @return maximum number of timer driven threads this NiFi has available
     */
    @ApiModelProperty(
            value = "The maximum number of timer driven threads the NiFi has available."
    )
    public Integer getMaxTimerDrivenThreadCount() {
        return maxTimerDrivenThreadCount;
    }

    public void setMaxTimerDrivenThreadCount(Integer maxTimerDrivenThreadCount) {
        this.maxTimerDrivenThreadCount = maxTimerDrivenThreadCount;
    }

    /**
     * @return maximum number of event driven thread this NiFi has available
     */
    @ApiModelProperty(
            value = "The maximum number of event driven threads the NiFi has available."
    )
    public Integer getMaxEventDrivenThreadCount() {
        return maxEventDrivenThreadCount;
    }

    public void setMaxEventDrivenThreadCount(Integer maxEventDrivenThreadCount) {
        this.maxEventDrivenThreadCount = maxEventDrivenThreadCount;
    }
}
