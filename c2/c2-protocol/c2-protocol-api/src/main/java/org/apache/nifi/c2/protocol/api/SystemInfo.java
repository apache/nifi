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
import java.util.Objects;

@ApiModel
public class SystemInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    @ApiModelProperty("Machine architecture of the device, e.g., ARM, x86")
    private String machineArch;

    private String operatingSystem;

    @ApiModelProperty(value = "Size of physical memory of the device in bytes", allowableValues = "range[0, 9223372036854775807]")
    private long physicalMem;

    @ApiModelProperty(
            value = "Number of virtual cores on the device",
            name = "vCores",
            allowableValues = "range[0, 2147483647]")
    private int vCores;

    @ApiModelProperty
    private long memoryUsage;

    @ApiModelProperty
    private double cpuUtilization;

    // See note on field for why vCores accessors do not use Lombok
    public int getvCores() {
        return vCores;
    }

    public void setvCores(int vCores) {
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

    public long getPhysicalMem() {
        return physicalMem;
    }

    public void setPhysicalMem(long physicalMem) {
        this.physicalMem = physicalMem;
    }

    public long getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(long memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public double getCpuUtilization() {
        return cpuUtilization;
    }

    public void setCpuUtilization(double cpuUtilization) {
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
