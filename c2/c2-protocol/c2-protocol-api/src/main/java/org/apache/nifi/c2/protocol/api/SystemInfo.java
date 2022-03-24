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

package org.apache.nifi.c2.protocol.api;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.io.Serializable;

@ApiModel
public class SystemInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    @ApiModelProperty("Machine architecture of the device, e.g., ARM, x86")
    private String machineArch;

    private String operatingSystem;

    @ApiModelProperty(value = "Size of physical memory of the device in bytes", allowableValues = "range[0, 9223372036854775807]")
    private Long physicalMem;

    @ApiModelProperty(
            value = "Number of virtual cores on the device",
            name = "vCores",
            allowableValues = "range[0, 2147483647]")
    private Integer vCores;

    @ApiModelProperty
    private Long memoryUsage;

    @ApiModelProperty
    private Double cpuUtilization;

    public String getMachineArch() {
        return machineArch;
    }

    public void setMachineArch(String machineArch) {
        this.machineArch = machineArch;
    }

    public String getOperatingSystem() {
        return operatingSystem;
    }

    public void setOperatingSystem(String operatingSystem) {
        this.operatingSystem = operatingSystem;
    }

    public Long getPhysicalMem() {
        return physicalMem;
    }

    public void setPhysicalMem(Long physicalMem) {
        this.physicalMem = physicalMem;
    }

    public Long getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(Long memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public Double getCpuUtilization() {
        return cpuUtilization;
    }

    public void setCpuUtilization(Double cpuUtilization) {
        this.cpuUtilization = cpuUtilization;
    }

    public Integer getvCores() {
        return vCores;
    }

    public void setvCores(Integer vCores) {
        this.vCores = vCores;
    }
}
