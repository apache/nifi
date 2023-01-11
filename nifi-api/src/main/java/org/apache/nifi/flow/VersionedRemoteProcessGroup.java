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

package org.apache.nifi.flow;

import io.swagger.annotations.ApiModelProperty;

import java.util.Set;

public class VersionedRemoteProcessGroup extends VersionedComponent {
    private String targetUri;
    private String targetUris;

    private String communicationsTimeout;
    private String yieldDuration;
    private String transportProtocol;
    private String localNetworkInterface;
    private String proxyHost;
    private Integer proxyPort;
    private String proxyUser;
    private String proxyPassword;

    private Set<VersionedRemoteGroupPort> inputPorts;
    private Set<VersionedRemoteGroupPort> outputPorts;


    @Deprecated
    @ApiModelProperty(
            value = "[DEPRECATED] The target URI of the remote process group." +
                    " If target uri is not set, but uris are set, then returns the first uri in the uris." +
                    " If neither target uri nor uris are set, then returns null.",
            notes = "This field is deprecated and will be removed in version 1.x of NiFi Registry." +
                    " Please migrate to using targetUris only.")
    public String getTargetUri() {

        if (!isEmpty(targetUri)) {
            return targetUri;
        }
        return !isEmpty(targetUris) ? targetUris.split(",", 2)[0] : null;

    }

    public void setTargetUri(final String targetUri) {
        this.targetUri = targetUri;
    }

    @ApiModelProperty(
            value = "The target URIs of the remote process group." +
                    " If target uris is not set but target uri is set, then returns the single target uri." +
                    " If neither target uris nor target uri is set, then returns null.")
    public String getTargetUris() {

        if (!isEmpty(targetUris)) {
            return targetUris;
        }
        return !isEmpty(targetUri) ? targetUri : null;

    }

    private boolean isEmpty(final String value) {
        return (value == null || value.isEmpty());
    }

    public void setTargetUris(String targetUris) {
        this.targetUris = targetUris;
    }

    @ApiModelProperty("The time period used for the timeout when communicating with the target.")
    public String getCommunicationsTimeout() {
        return communicationsTimeout;
    }

    public void setCommunicationsTimeout(String communicationsTimeout) {
        this.communicationsTimeout = communicationsTimeout;
    }

    @ApiModelProperty("When yielding, this amount of time must elapse before the remote process group is scheduled again.")
    public String getYieldDuration() {
        return yieldDuration;
    }

    public void setYieldDuration(String yieldDuration) {
        this.yieldDuration = yieldDuration;
    }

    @ApiModelProperty(value = "The Transport Protocol that is used for Site-to-Site communications", allowableValues = "RAW, HTTP")
    public String getTransportProtocol() {
        return transportProtocol;
    }

    public void setTransportProtocol(String transportProtocol) {
        this.transportProtocol = transportProtocol;
    }

    @ApiModelProperty("A Set of Input Ports that can be connected to, in order to send data to the remote NiFi instance")
    public Set<VersionedRemoteGroupPort> getInputPorts() {
        return inputPorts;
    }

    public void setInputPorts(Set<VersionedRemoteGroupPort> inputPorts) {
        this.inputPorts = inputPorts;
    }

    @ApiModelProperty("A Set of Output Ports that can be connected to, in order to pull data from the remote NiFi instance")
    public Set<VersionedRemoteGroupPort> getOutputPorts() {
        return outputPorts;
    }

    public void setOutputPorts(Set<VersionedRemoteGroupPort> outputPorts) {
        this.outputPorts = outputPorts;
    }


    @ApiModelProperty("The local network interface to send/receive data. If not specified, any local address is used. If clustered, all nodes must have an interface with this identifier.")
    public String getLocalNetworkInterface() {
        return localNetworkInterface;
    }

    public void setLocalNetworkInterface(String localNetworkInterface) {
        this.localNetworkInterface = localNetworkInterface;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    public Integer getProxyPort() {
        return proxyPort;
    }

    public void setProxyPort(Integer proxyPort) {
        this.proxyPort = proxyPort;
    }

    public String getProxyUser() {
        return proxyUser;
    }

    public void setProxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    @Override
    public ComponentType getComponentType() {
        return ComponentType.REMOTE_PROCESS_GROUP;
    }
}
