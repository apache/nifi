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

import java.util.Date;
import java.util.List;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.DateTimeAdapter;

/**
 * Details of a remote process group in this NiFi.
 */
@XmlType(name = "remoteProcessGroup")
public class RemoteProcessGroupDTO extends NiFiComponentDTO {

    private String targetUri;
    private Boolean targetSecure;

    private String name;
    private String comments;
    private String communicationsTimeout;
    private String yieldDuration;

    private List<String> authorizationIssues;
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
     * The target uri of this remote process group.
     *
     * @return
     */
    public String getTargetUri() {
        return this.targetUri;
    }

    /**
     * The name of this remote process group.
     *
     * @param name
     */
    public void setName(final String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    /**
     * Comments for this remote process group.
     *
     * @return
     */
    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    /**
     * Returns any remote authorization issues for this remote process group.
     *
     * @return
     */
    public List<String> getAuthorizationIssues() {
        return authorizationIssues;
    }

    public void setAuthorizationIssues(List<String> authorizationIssues) {
        this.authorizationIssues = authorizationIssues;
    }

    /**
     * Whether or not this remote process group is actively transmitting.
     *
     * @return
     */
    public Boolean isTransmitting() {
        return transmitting;
    }

    public void setTransmitting(Boolean transmitting) {
        this.transmitting = transmitting;
    }

    /**
     * Whether or not the target is running securely.
     *
     * @return
     */
    public Boolean isTargetSecure() {
        return targetSecure;
    }

    public void setTargetSecure(Boolean targetSecure) {
        this.targetSecure = targetSecure;
    }

    /**
     * Returns the time period used for the timeout when communicating with this
     * RemoteProcessGroup.
     *
     * @return
     */
    public String getCommunicationsTimeout() {
        return communicationsTimeout;
    }

    public void setCommunicationsTimeout(String communicationsTimeout) {
        this.communicationsTimeout = communicationsTimeout;
    }

    /**
     * When yielding, this amount of time must elaspe before this remote process
     * group is scheduled again.
     *
     * @return
     */
    public String getYieldDuration() {
        return yieldDuration;
    }

    public void setYieldDuration(String yieldDuration) {
        this.yieldDuration = yieldDuration;
    }

    /**
     * The number of active remote input ports.
     *
     * @return
     */
    public Integer getActiveRemoteInputPortCount() {
        return activeRemoteInputPortCount;
    }

    public void setActiveRemoteInputPortCount(Integer activeRemoteInputPortCount) {
        this.activeRemoteInputPortCount = activeRemoteInputPortCount;
    }

    /**
     * The number of inactive remote input ports.
     *
     * @return
     */
    public Integer getInactiveRemoteInputPortCount() {
        return inactiveRemoteInputPortCount;
    }

    public void setInactiveRemoteInputPortCount(Integer inactiveRemoteInputPortCount) {
        this.inactiveRemoteInputPortCount = inactiveRemoteInputPortCount;
    }

    /**
     * The number of active remote output ports.
     *
     * @return
     */
    public Integer getActiveRemoteOutputPortCount() {
        return activeRemoteOutputPortCount;
    }

    public void setActiveRemoteOutputPortCount(Integer activeRemoteOutputPortCount) {
        this.activeRemoteOutputPortCount = activeRemoteOutputPortCount;
    }

    /**
     * The number of inactive remote output ports.
     *
     * @return
     */
    public Integer getInactiveRemoteOutputPortCount() {
        return inactiveRemoteOutputPortCount;
    }

    public void setInactiveRemoteOutputPortCount(Integer inactiveRemoteOutputPortCount) {
        this.inactiveRemoteOutputPortCount = inactiveRemoteOutputPortCount;
    }

    /**
     * The number of Remote Input Ports currently available in the remote NiFi
     * instance
     *
     * @return
     */
    public Integer getInputPortCount() {
        return inputPortCount;
    }

    public void setInputPortCount(Integer inputPortCount) {
        this.inputPortCount = inputPortCount;
    }

    /**
     * The number of Remote Output Ports currently available in the remote NiFi
     * instance
     *
     * @return
     */
    public Integer getOutputPortCount() {
        return outputPortCount;
    }

    public void setOutputPortCount(Integer outputPortCount) {
        this.outputPortCount = outputPortCount;
    }

    /**
     * The contents of this remote process group. Will contain available
     * input/output ports.
     *
     * @return
     */
    public RemoteProcessGroupContentsDTO getContents() {
        return contents;
    }

    public void setContents(RemoteProcessGroupContentsDTO contents) {
        this.contents = contents;
    }

    /**
     * When the flow for this remote group was last refreshed.
     *
     * @return
     */
    @XmlJavaTypeAdapter(DateTimeAdapter.class)
    public Date getFlowRefreshed() {
        return flowRefreshed;
    }

    public void setFlowRefreshed(Date flowRefreshed) {
        this.flowRefreshed = flowRefreshed;
    }

}
