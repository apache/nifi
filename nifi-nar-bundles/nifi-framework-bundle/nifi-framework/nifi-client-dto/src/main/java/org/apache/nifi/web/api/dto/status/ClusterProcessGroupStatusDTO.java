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
import java.util.Collection;
import java.util.Date;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

/**
 * DTO for serializing the a process group's status across the cluster.
 */
@XmlType(name = "clusterProcessGroupStatus")
public class ClusterProcessGroupStatusDTO {

    private Collection<NodeProcessGroupStatusDTO> nodeProcessGroupStatus;
    private Date statsLastRefreshed;
    private String processGroupId;
    private String processGroupName;

    /**
     * The time the status were last refreshed.
     *
     * @return The time the status were last refreshed
     */
    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
            value = "The time when the stats was last refreshed."
    )
    public Date getStatsLastRefreshed() {
        return statsLastRefreshed;
    }

    public void setStatsLastRefreshed(Date statsLastRefreshed) {
        this.statsLastRefreshed = statsLastRefreshed;
    }

    /**
     * The process group id.
     *
     * @return The process group id
     */
    @ApiModelProperty(
            value = "The id of the process group."
    )
    public String getProcessGroupId() {
        return processGroupId;
    }

    public void setProcessGroupId(String processGroupId) {
        this.processGroupId = processGroupId;
    }

    /**
     * The process group name.
     *
     * @return The process group name
     */
    @ApiModelProperty(
            value = "The name of the process group."
    )
    public String getProcessGroupName() {
        return processGroupName;
    }

    public void setProcessGroupName(String processGroupName) {
        this.processGroupName = processGroupName;
    }

    /**
     * Collection of node process group status DTO.
     *
     * @return The collection of node process group status DTO
     */
    @ApiModelProperty(
            value = "The process groups status for each node."
    )
    public Collection<NodeProcessGroupStatusDTO> getNodeProcessGroupStatus() {
        return nodeProcessGroupStatus;
    }

    public void setNodeProcessGroupStatus(Collection<NodeProcessGroupStatusDTO> nodeProcessGroupStatus) {
        this.nodeProcessGroupStatus = nodeProcessGroupStatus;
    }

}
