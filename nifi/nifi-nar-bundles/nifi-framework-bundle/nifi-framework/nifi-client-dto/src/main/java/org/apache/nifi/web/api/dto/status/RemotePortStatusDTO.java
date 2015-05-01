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
package org.apache.nifi.web.api.dto.status;

import com.wordnik.swagger.annotations.ApiModelProperty;
import javax.xml.bind.annotation.XmlType;

/**
 * The status of a Port on a remote NiFi instance.
 */
@XmlType(name = "remotePortStatus")
public class RemotePortStatusDTO {

    private String id;
    private String connectionId;
    private String name;
    private Boolean running;
    private Boolean exists;

    /**
     * @return id of the connection this remote port is connected to
     */
    @ApiModelProperty(
            value = "The id of the conneciton the remote is connected to."
    )
    public String getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(String connectionId) {
        this.connectionId = connectionId;
    }

    /**
     * @return id of the remote port
     */
    @ApiModelProperty(
            value = "The id of the remote port."
    )
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return name of the remote port
     */
    @ApiModelProperty(
            value = "The name of the remote port."
    )
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return whether or not the remote port exists
     */
    @ApiModelProperty(
            value = "Whether or not the remote port exists."
    )
    public Boolean getExists() {
        return exists;
    }

    public void setExists(Boolean exists) {
        this.exists = exists;
    }

    /**
     * @return whether or not the remote port is running
     */
    @ApiModelProperty(
            value = "Whether or not the remote port is running."
    )
    public Boolean getRunning() {
        return running;
    }

    public void setRunning(Boolean running) {
        this.running = running;
    }

}
