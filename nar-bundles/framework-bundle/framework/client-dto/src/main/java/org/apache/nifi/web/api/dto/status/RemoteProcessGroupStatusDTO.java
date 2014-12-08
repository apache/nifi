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

import java.util.List;
import javax.xml.bind.annotation.XmlType;

/**
 * The status of a remote process group in this NiFi.
 */
@XmlType(name = "remoteProcessGroupStatus")
public class RemoteProcessGroupStatusDTO extends StatusDTO {

    private String id;
    private String groupId;
    private String name;
    private String targetUri;
    private String transmissionStatus;
    private Integer activeThreadCount;

    private List<String> authorizationIssues;

    private String sent;
    private String received;

    /**
     * The id for the remote process group.
     *
     * @return The id for the remote process group
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * The id of the group this remote process group is in.
     *
     * @return
     */
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    /**
     * The URI of the target system.
     *
     * @return
     */
    public String getTargetUri() {
        return targetUri;
    }

    public void setTargetUri(String targetUri) {
        this.targetUri = targetUri;
    }

    /**
     * The name of this remote process group.
     *
     * @return
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * The transmission status of this remote process group.
     *
     * @return
     */
    public String getTransmissionStatus() {
        return transmissionStatus;
    }

    public void setTransmissionStatus(String transmissionStatus) {
        this.transmissionStatus = transmissionStatus;
    }

    /**
     * The number of active threads.
     *
     * @return
     */
    public Integer getActiveThreadCount() {
        return activeThreadCount;
    }

    public void setActiveThreadCount(Integer activeThreadCount) {
        this.activeThreadCount = activeThreadCount;
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
     * Formatted description of the amount of data sent to this remote process
     * group.
     *
     * @return
     */
    public String getSent() {
        return sent;
    }

    public void setSent(String sent) {
        this.sent = sent;
    }

    /**
     * Formatted description of the amount of data received from this remote
     * process group.
     *
     * @return
     */
    public String getReceived() {
        return received;
    }

    public void setReceived(String received) {
        this.received = received;
    }

}
