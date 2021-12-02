/*
 * Apache NiFi - MiNiFi
 * Copyright 2014-2018 The Apache Software Foundation
 *
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

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Objects;

@ApiModel
public class SystemInfo implements Serializable {
    private static final long serialVersionUID = 830834593998474779L;

    public static final int MACHINE_ARCH_MAX_SIZE = 100;
    public static final int OS_MAX_SIZE = 100;

    @Size(max = MACHINE_ARCH_MAX_SIZE)
    @ApiModelProperty("Machine architecture of the device, e.g., ARM, x86")
    private String machineArch;

    @Size(max = OS_MAX_SIZE)
    private String operatingSystem;

    @Min(0)
    @Max(Long.MAX_VALUE)
    @ApiModelProperty(value = "Size of physical memory of the device in bytes", allowableValues = "range[0, 9223372036854775807]")
    private Long physicalMem;

    @Min(0)
    @Max(Integer.MAX_VALUE)
    @ApiModelProperty(
            value = "Number of virtual cores on the device",
            name = "vCores",
            allowableValues = "range[0, 2147483647]")
    private Integer vCores;

    @Min(value = -1, message = "Was not able to determine memory usage of the system")
    @Max(Long.MAX_VALUE)
    @ApiModelProperty
    private Long memoryUsage;

    @DecimalMin(value = "-1.0", message = "Was not able to determine CPU utilisation of the system")
    @DecimalMax("1.0")
    @ApiModelProperty
    private Double cpuUtilization;

    // See note on field for why vCores accessors do not use Lombok
    public Integer getvCores() {
        return vCores;
    }

    public void setvCores(Integer vCores) {
        this.vCores = vCores;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SystemInfo that = (SystemInfo) o;
        return Objects.equals(machineArch, that.machineArch)
                && Objects.equals(operatingSystem, that.operatingSystem)
                && Objects.equals(physicalMem, that.physicalMem)
                && Objects.equals(vCores, that.vCores)
                && Objects.equals(memoryUsage, that.memoryUsage)
                && Objects.equals(cpuUtilization, that.cpuUtilization);
    }

    @Override
    public int hashCode() {
        return Objects.hash(machineArch, operatingSystem, physicalMem, vCores, memoryUsage, cpuUtilization);
    }
}
