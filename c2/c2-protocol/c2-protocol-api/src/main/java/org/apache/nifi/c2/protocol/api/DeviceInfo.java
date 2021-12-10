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
public class DeviceInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    @ApiModelProperty(
        value = "A unique, long-lived identifier for the device",
        required = true)
    private String identifier;

    @ApiModelProperty("Metadata for the device hardware and operating system")
    private SystemInfo systemInfo;

    @ApiModelProperty("Metadata for the network interface of this device")
    private NetworkInfo networkInfo;

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public SystemInfo getSystemInfo() {
        return systemInfo;
    }

    public void setSystemInfo(SystemInfo systemInfo) {
        this.systemInfo = systemInfo;
    }

    public NetworkInfo getNetworkInfo() {
        return networkInfo;
    }

    public void setNetworkInfo(NetworkInfo networkInfo) {
        this.networkInfo = networkInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeviceInfo that = (DeviceInfo) o;
        return Objects.equals(identifier, that.identifier) && Objects.equals(systemInfo, that.systemInfo) && Objects.equals(networkInfo, that.networkInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, systemInfo, networkInfo);
    }
}
