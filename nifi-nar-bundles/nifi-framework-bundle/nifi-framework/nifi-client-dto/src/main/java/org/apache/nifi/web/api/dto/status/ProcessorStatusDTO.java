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
import org.apache.nifi.web.api.dto.util.TimeAdapter;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * DTO for serializing the status of a processor.
 */
@XmlType(name = "processorStatus")
public class ProcessorStatusDTO implements Cloneable {
    private String groupId;
    private String id;
    private String name;
    private String type;
    private String runStatus;
    private Date statsLastRefreshed;

    private ProcessorStatusSnapshotDTO aggregateSnapshot;
    private List<NodeProcessorStatusSnapshotDTO> nodeSnapshots;

    @ApiModelProperty("The unique ID of the process group that the Processor belongs to")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @ApiModelProperty("The unique ID of the Processor")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty("The name of the Processor")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty("The type of the Processor")
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @ApiModelProperty("The run status of the Processor")
    public String getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(String runStatus) {
        this.runStatus = runStatus;
    }

    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
            value="The timestamp of when the stats were last refreshed",
            dataType = "string"
    )
    public Date getStatsLastRefreshed() {
        return statsLastRefreshed;
    }

    public void setStatsLastRefreshed(Date statsLastRefreshed) {
        this.statsLastRefreshed = statsLastRefreshed;
    }

    @ApiModelProperty("A status snapshot that represents the aggregate stats of all nodes in the cluster. If the NiFi instance is "
        + "a standalone instance, rather than a cluster, this represents the stats of the single instance.")
    public ProcessorStatusSnapshotDTO getAggregateSnapshot() {
        return aggregateSnapshot;
    }

    public void setAggregateSnapshot(ProcessorStatusSnapshotDTO aggregateSnapshot) {
        this.aggregateSnapshot = aggregateSnapshot;
    }

    @ApiModelProperty("A status snapshot for each node in the cluster. If the NiFi instance is a standalone instance, rather than "
        + "a cluster, this may be null.")
    public List<NodeProcessorStatusSnapshotDTO> getNodeSnapshots() {
        return nodeSnapshots;
    }

    public void setNodeSnapshots(List<NodeProcessorStatusSnapshotDTO> nodeSnapshots) {
        this.nodeSnapshots = nodeSnapshots;
    }

    @Override
    public ProcessorStatusDTO clone() {
        final ProcessorStatusDTO other = new ProcessorStatusDTO();
        other.setGroupId(getGroupId());
        other.setId(getId());
        other.setName(getName());
        other.setRunStatus(getRunStatus());
        other.setType(getType());
        other.setStatsLastRefreshed(getStatsLastRefreshed());
        other.setAggregateSnapshot(getAggregateSnapshot().clone());

        final List<NodeProcessorStatusSnapshotDTO> nodeStatuses = getNodeSnapshots();
        final List<NodeProcessorStatusSnapshotDTO> nodeStatusClones = new ArrayList<>(nodeStatuses.size());
        for (final NodeProcessorStatusSnapshotDTO status : nodeStatuses) {
            nodeStatusClones.add(status.clone());
        }
        other.setNodeSnapshots(nodeStatusClones);

        return other;
    }
}
