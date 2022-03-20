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

package org.apache.nifi.c2.protocol.component.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@ApiModel
public class ReportingTaskDefinition extends ConfigurableExtensionDefinition {
    private static final long serialVersionUID = 1L;

    private List<String> supportedSchedulingStrategies;
    private String defaultSchedulingStrategy;
    private Map<String, String> defaultSchedulingPeriodBySchedulingStrategy;

    @ApiModelProperty("The supported scheduling strategies, such as TIME_DRIVER or CRON.")
    public List<String> getSupportedSchedulingStrategies() {
        return (supportedSchedulingStrategies != null ? Collections.unmodifiableList(supportedSchedulingStrategies) : null);
    }

    public void setSupportedSchedulingStrategies(List<String> supportedSchedulingStrategies) {
        this.supportedSchedulingStrategies = supportedSchedulingStrategies;
    }

    @ApiModelProperty("The default scheduling strategy for the reporting task.")
    public String getDefaultSchedulingStrategy() {
        return defaultSchedulingStrategy;
    }

    public void setDefaultSchedulingStrategy(String defaultSchedulingStrategy) {
        this.defaultSchedulingStrategy = defaultSchedulingStrategy;
    }

    @ApiModelProperty("The default scheduling period for each scheduling strategy. " +
            "The scheduling period is expected to be a time period, such as \"30 sec\".")
    public Map<String, String> getDefaultSchedulingPeriodBySchedulingStrategy() {
        return defaultSchedulingPeriodBySchedulingStrategy != null ? Collections.unmodifiableMap(defaultSchedulingPeriodBySchedulingStrategy) : null;
    }

    public void setDefaultSchedulingPeriodBySchedulingStrategy(Map<String, String> defaultSchedulingPeriodBySchedulingStrategy) {
        this.defaultSchedulingPeriodBySchedulingStrategy = defaultSchedulingPeriodBySchedulingStrategy;
    }

}
