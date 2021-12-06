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
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

@ApiModel
public class SchedulingDefaults implements Serializable {
    private static final long serialVersionUID = 1L;

    private SchedulingStrategy defaultSchedulingStrategy;
    private long defaultSchedulingPeriodMillis;
    private long penalizationPeriodMillis;
    private long yieldDurationMillis;
    private long defaultRunDurationNanos;
    private String defaultMaxConcurrentTasks;

    private Map<String, Integer> defaultConcurrentTasksBySchedulingStrategy;
    private Map<String, String> defaultSchedulingPeriodsBySchedulingStrategy;

    @ApiModelProperty("The name of the default scheduling strategy")
    public SchedulingStrategy getDefaultSchedulingStrategy() {
        return defaultSchedulingStrategy;
    }

    public void setDefaultSchedulingStrategy(SchedulingStrategy defaultSchedulingStrategy) {
        this.defaultSchedulingStrategy = defaultSchedulingStrategy;
    }

    @ApiModelProperty("The default scheduling period in milliseconds")
    public long getDefaultSchedulingPeriodMillis() {
        return defaultSchedulingPeriodMillis;
    }

    public void setDefaultSchedulingPeriodMillis(long defaultSchedulingPeriodMillis) {
        this.defaultSchedulingPeriodMillis = defaultSchedulingPeriodMillis;
    }

    @ApiModelProperty("The default penalization period in milliseconds")
    public long getPenalizationPeriodMillis() {
        return penalizationPeriodMillis;
    }

    public void setPenalizationPeriodMillis(long penalizationPeriodMillis) {
        this.penalizationPeriodMillis = penalizationPeriodMillis;
    }

    @ApiModelProperty("The default yield duration in milliseconds")
    public long getYieldDurationMillis() {
        return yieldDurationMillis;
    }

    public void setYieldDurationMillis(long yieldDurationMillis) {
        this.yieldDurationMillis = yieldDurationMillis;
    }

    @ApiModelProperty("The default run duration in nano-seconds")
    public long getDefaultRunDurationNanos() {
        return defaultRunDurationNanos;
    }

    public void setDefaultRunDurationNanos(long defaultRunDurationNanos) {
        this.defaultRunDurationNanos = defaultRunDurationNanos;
    }

    @ApiModelProperty("The default concurrent tasks")
    public String getDefaultMaxConcurrentTasks() {
        return defaultMaxConcurrentTasks;
    }

    public void setDefaultMaxConcurrentTasks(String defaultMaxConcurrentTasks) {
        this.defaultMaxConcurrentTasks = defaultMaxConcurrentTasks;
    }

    @ApiModelProperty("The default concurrent tasks for each scheduling strategy")
    public Map<String, Integer> getDefaultConcurrentTasksBySchedulingStrategy() {
        return defaultConcurrentTasksBySchedulingStrategy != null ? Collections.unmodifiableMap(defaultConcurrentTasksBySchedulingStrategy) : null;
    }

    public void setDefaultConcurrentTasksBySchedulingStrategy(Map<String, Integer> defaultConcurrentTasksBySchedulingStrategy) {
        this.defaultConcurrentTasksBySchedulingStrategy = defaultConcurrentTasksBySchedulingStrategy;
    }

    @ApiModelProperty("The default scheduling period for each scheduling strategy")
    public Map<String, String> getDefaultSchedulingPeriodsBySchedulingStrategy() {
        return defaultSchedulingPeriodsBySchedulingStrategy != null ? Collections.unmodifiableMap(defaultSchedulingPeriodsBySchedulingStrategy) : null;
    }

    public void setDefaultSchedulingPeriodsBySchedulingStrategy(Map<String, String> defaultSchedulingPeriodsBySchedulingStrategy) {
        this.defaultSchedulingPeriodsBySchedulingStrategy = defaultSchedulingPeriodsBySchedulingStrategy;
    }

}
