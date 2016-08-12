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
 * Details for the access configuration.
 */
@XmlType(name = "permission")
public class PermissionsDTO implements ReadablePermission, WritablePermission {

    private boolean canRead = false;
    private boolean canWrite = false;

    /**
     * @return Indicates whether the user can read a given resource.
     */
    @ApiModelProperty(
            value = "Indicates whether the user can read a given resource.",
            readOnly = true
    )
    @Override
    public Boolean getCanRead() {
        return canRead;
    }

    @Override
    public void setCanRead(Boolean canRead) {
        this.canRead = canRead;
    }

    /**
     * @return Indicates whether the user can write a given resource.
     */
    @ApiModelProperty(
            value = "Indicates whether the user can write a given resource.",
            readOnly = true
    )
    @Override
    public Boolean getCanWrite() {
        return canWrite;
    }

    @Override
    public void setCanWrite(Boolean canWrite) {
        this.canWrite = canWrite;
    }
}
