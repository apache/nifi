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
import javax.validation.constraints.Size;
import java.util.Objects;

@ApiModel
public class Device extends DeviceInfo {
    private static final long serialVersionUID = 8897272414778863539L;

    private static final int NAME_MAX_SIZE = 200;

    @ApiModelProperty
    @Size(max = NAME_MAX_SIZE)
    private String name;

    @ApiModelProperty("A timestamp (milliseconds since Epoch) for the first time the device was seen by this C2 server")
    private Long firstSeen;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Device device = (Device) o;
        return Objects.equals(name, device.name) && Objects.equals(firstSeen, device.firstSeen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, firstSeen);
    }
}
