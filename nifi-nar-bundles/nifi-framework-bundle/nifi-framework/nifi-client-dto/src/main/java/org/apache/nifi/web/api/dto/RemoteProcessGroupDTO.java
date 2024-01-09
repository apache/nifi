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

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.nifi.web.api.dto.util.DateTimeAdapter;

import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Collection;
import java.util.Date;

/**
 * Details of a remote process group in this NiFi.
 */
@XmlType(name = "remoteProcessGroup")
public class RemoteProcessGroupDTO extends ComponentDTO {

    private String targetUri;
    private String targetUris;
    private Boolean targetSecure;

    private String name;
    private String comments;
    private String communicationsTimeout;
    private String yieldDuration;
    private String transportProtocol;
    private String localNetworkInterface;
    private String proxyHost;
    private Integer proxyPort;
    private String proxyUser;
    private String proxyPassword;

    private Collection<String> authorizationIssues;
    private Collection<String> validationErrors;
    private Boolean transmitting;

    private Integer inputPortCount;
    private Integer outputPortCount;

    private Integer activeRemoteInputPortCount;
    private Integer inactiveRemoteInputPortCount;
    private Integer activeRemoteOutputPortCount;
    private Integer inactiveRemoteOutputPortCount;

    private Date flowRefreshed;

    private RemoteProcessGroupContentsDTO contents;

    public RemoteProcessGroupDTO() {
        super();
    }

    public RemoteProcessGroupDTO(final RemoteProcessGroupDTO toCopy) {
        setId(toCopy.getId());
        setPosition(toCopy.getPosition());
        targetUri = toCopy.getTargetUri();
        name = toCopy.getName();
    }

    public void setTargetUri(final String targetUri) {
        this.targetUri = targetUri;
    }

    /**
     * @return target uri of this remote process group.
     * If target uri is not set, but uris are set, then returns the first url in the urls.
     * If neither target uri nor uris are set, then returns null.
     */
    @Schema(description = "The target URI of the remote process group." +
                    " If target uri is not set, but uris are set, then returns the first url in the urls." +
                    " If neither target uri nor uris are set, then returns null."
    )
    public String getTargetUri() {
        if (targetUri == null || targetUri.length() == 0) {
            synchronized (this) {
                if (targetUri == null || targetUri.length() == 0) {
                    if (targetUris != null && targetUris.length() > 0) {
                        if (targetUris.indexOf(',') > -1) {
                            targetUri = targetUris.substring(0, targetUris.indexOf(','));
                        } else {
                            targetUri = targetUris;
                        }
                    }
                }
            }
        }

        return this.targetUri;
    }

    public void setTargetUris(String targetUris) {
        this.targetUris = targetUris;
    }

    /**
     * @return target uris of this remote process group
     * If targetUris was not set but target uri was set, then returns a collection containing the single uri.
     * If neither target uris nor uri were set, then returns null.
     */
    @Schema(description = "The target URI of the remote process group." +
                    " If target uris is not set but target uri is set," +
                    " then returns a collection containing the single target uri." +
                    " If neither target uris nor uris are set, then returns null."
    )
    public String getTargetUris() {
        if (targetUris == null || targetUris.length() == 0) {
            synchronized (this) {
                if (targetUris == null || targetUris.length() == 0) {
                    targetUris = targetUri;
                }
            }
        }

        return this.targetUris;
    }

    /**
     * @param name of this remote process group
     */
    @Schema(description = "The name of the remote process group."
    )
    public void setName(final String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    /**
     * @return Comments for this remote process group
     */
    @Schema(description = "The comments for the remote process group."
    )
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * @return any remote authorization issues for this remote process group
     */
    @Schema(description = "Any remote authorization issues for the remote process group."
    )
    public Collection<String> getAuthorizationIssues() {
        return authorizationIssues;
    }

    public void setAuthorizationIssues(Collection<String> authorizationIssues) {
        this.authorizationIssues = authorizationIssues;
    }

    /**
     * @return whether or not this remote process group is actively transmitting
     */
    @Schema(description = "Whether the remote process group is actively transmitting."
    )
    public Boolean isTransmitting() {
        return transmitting;
    }

    public void setTransmitting(Boolean transmitting) {
        this.transmitting = transmitting;
    }

    /**
     * @return whether or not the target is running securely
     */
    @Schema(description = "Whether the target is running securely."
    )
    public Boolean isTargetSecure() {
        return targetSecure;
    }

    public void setTargetSecure(Boolean targetSecure) {
        this.targetSecure = targetSecure;
    }

    /**
     * @return the time period used for the timeout when communicating with this RemoteProcessGroup
     */
    @Schema(description = "The time period used for the timeout when communicating with the target."
    )
    public String getCommunicationsTimeout() {
        return communicationsTimeout;
    }

    public void setCommunicationsTimeout(String communicationsTimeout) {
        this.communicationsTimeout = communicationsTimeout;
    }

    /**
     * @return when yielding, this amount of time must elapse before this remote process group is scheduled again
     */
    @Schema(description = "When yielding, this amount of time must elapse before the remote process group is scheduled again."
    )
    public String getYieldDuration() {
        return yieldDuration;
    }

    public void setYieldDuration(String yieldDuration) {
        this.yieldDuration = yieldDuration;
    }

    /**
     * @return number of active remote input ports
     */
    @Schema(description = "The number of active remote input ports."
    )
    public Integer getActiveRemoteInputPortCount() {
        return activeRemoteInputPortCount;
    }

    public void setActiveRemoteInputPortCount(Integer activeRemoteInputPortCount) {
        this.activeRemoteInputPortCount = activeRemoteInputPortCount;
    }

    /**
     * @return number of inactive remote input ports
     */
    @Schema(description = "The number of inactive remote input ports."
    )
    public Integer getInactiveRemoteInputPortCount() {
        return inactiveRemoteInputPortCount;
    }

    public void setInactiveRemoteInputPortCount(Integer inactiveRemoteInputPortCount) {
        this.inactiveRemoteInputPortCount = inactiveRemoteInputPortCount;
    }

    /**
     * @return number of active remote output ports
     */
    @Schema(description = "The number of active remote output ports."
    )
    public Integer getActiveRemoteOutputPortCount() {
        return activeRemoteOutputPortCount;
    }

    public void setActiveRemoteOutputPortCount(Integer activeRemoteOutputPortCount) {
        this.activeRemoteOutputPortCount = activeRemoteOutputPortCount;
    }

    /**
     * @return number of inactive remote output ports
     */
    @Schema(description = "The number of inactive remote output ports."
    )
    public Integer getInactiveRemoteOutputPortCount() {
        return inactiveRemoteOutputPortCount;
    }

    public void setInactiveRemoteOutputPortCount(Integer inactiveRemoteOutputPortCount) {
        this.inactiveRemoteOutputPortCount = inactiveRemoteOutputPortCount;
    }

    /**
     * @return number of Remote Input Ports currently available in the remote NiFi instance
     */
    @Schema(description = "The number of remote input ports currently available on the target."
    )
    public Integer getInputPortCount() {
        return inputPortCount;
    }

    public void setInputPortCount(Integer inputPortCount) {
        this.inputPortCount = inputPortCount;
    }

    /**
     * @return number of Remote Output Ports currently available in the remote NiFi instance
     */
    @Schema(description = "The number of remote output ports currently available on the target."
    )
    public Integer getOutputPortCount() {
        return outputPortCount;
    }

    public void setOutputPortCount(Integer outputPortCount) {
        this.outputPortCount = outputPortCount;
    }

    /**
     * @return contents of this remote process group. Will contain available input/output ports
     */
    @Schema(description = "The contents of the remote process group. Will contain available input/output ports."
    )
    public RemoteProcessGroupContentsDTO getContents() {
        return contents;
    }

    public void setContents(RemoteProcessGroupContentsDTO contents) {
        this.contents = contents;
    }

    /**
     * @return the flow for this remote group was last refreshed
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    @Schema(description = "The timestamp when this remote process group was last refreshed.",
            type = "string"
    )
    public Date getFlowRefreshed() {
        return flowRefreshed;
    }

    public void setFlowRefreshed(Date flowRefreshed) {
        this.flowRefreshed = flowRefreshed;
    }

    public String getTransportProtocol() {
        return transportProtocol;
    }

    public void setTransportProtocol(String transportProtocol) {
        this.transportProtocol = transportProtocol;
    }

    @Schema(description = "The local network interface to send/receive data. If not specified, any local address is used. If clustered, all nodes must have an interface with this identifier.")
    public String getLocalNetworkInterface() {
        return localNetworkInterface;
    }

    public void setLocalNetworkInterface(String localNetworkInterface) {
        this.localNetworkInterface = localNetworkInterface;
    }

    @Schema(description = """
                          The validation errors for the remote process group.
                          These validation errors represent the problems with the remote process group that must be resolved before it can transmit.
                          """
    )
    public Collection<String> getValidationErrors() {
        return validationErrors;
    }

    public void setValidationErrors(Collection<String> validationErrors) {
        this.validationErrors = validationErrors;
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
}
