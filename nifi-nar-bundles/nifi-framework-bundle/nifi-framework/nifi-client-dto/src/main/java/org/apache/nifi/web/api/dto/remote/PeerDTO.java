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
package org.apache.nifi.web.api.dto.remote;

import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * Details of a Site-to-Site peer within this NiFi.
 */
@XmlType(name = "peer")
public class PeerDTO {

    private String hostname;
    private int port;
    private boolean secure;
    private int flowFileCount;

    @ApiModelProperty(
            value = "The hostname of this peer."
    )
    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    @ApiModelProperty(
            value = "The port number of this peer."
    )
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @ApiModelProperty(
            value = "Returns if this peer connection is secure."
    )
    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }


    @ApiModelProperty(
            value = "The number of flowFiles this peer holds."
    )
    public int getFlowFileCount() {
        return flowFileCount;
    }

    public void setFlowFileCount(int flowFileCount) {
        this.flowFileCount = flowFileCount;
    }
}
