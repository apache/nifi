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

@XmlType(name = "connectionStatus")
public class ConnectionStatusDTO implements Cloneable {
    private String id;
    private String groupId;
    private String name;
    private Date statsLastRefreshed;

    private String sourceId;
    private String sourceName;
    private String destinationId;

    private String destinationName;
    private ConnectionStatusSnapshotDTO aggregateSnapshot;

    private List<NodeConnectionStatusSnapshotDTO> nodeSnapshots;

    @ApiModelProperty("The ID of the connection")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty("The ID of the Process Group that the connection belongs to")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @ApiModelProperty("The name of the connection")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @ApiModelProperty("The ID of the source component")
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    @ApiModelProperty("The name of the source component")
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    @ApiModelProperty("The ID of the destination component")
    public String getDestinationId() {
        return destinationId;
    }

    public void setDestinationId(String destinationId) {
        this.destinationId = destinationId;
    }

    @ApiModelProperty("The name of the destination component")
    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    @ApiModelProperty("The status snapshot that represents the aggregate stats of the cluster")
    public ConnectionStatusSnapshotDTO getAggregateSnapshot() {
        return aggregateSnapshot;
    }

    public void setAggregateSnapshot(ConnectionStatusSnapshotDTO aggregateSnapshot) {
        this.aggregateSnapshot = aggregateSnapshot;
    }

    @ApiModelProperty("A list of status snapshots for each node")
    public List<NodeConnectionStatusSnapshotDTO> getNodeSnapshots() {
        return nodeSnapshots;
    }

    public void setNodeSnapshots(List<NodeConnectionStatusSnapshotDTO> nodeSnapshots) {
        this.nodeSnapshots = nodeSnapshots;
    }

    @XmlJavaTypeAdapter(TimeAdapter.class)
    @ApiModelProperty(
            value = "The timestamp of when the stats were last refreshed",
            dataType = "string"
    )
    public Date getStatsLastRefreshed() {
        return statsLastRefreshed;
    }

    public void setStatsLastRefreshed(Date statsLastRefreshed) {
        this.statsLastRefreshed = statsLastRefreshed;
    }

    @Override
    public ConnectionStatusDTO clone() {
        final ConnectionStatusDTO other = new ConnectionStatusDTO();
        other.setDestinationId(getDestinationId());
        other.setDestinationName(getDestinationName());
        other.setGroupId(getGroupId());
        other.setId(getId());
        other.setName(getName());
        other.setSourceId(getSourceId());
        other.setSourceName(getSourceName());
        other.setAggregateSnapshot(getAggregateSnapshot().clone());


        final List<NodeConnectionStatusSnapshotDTO> nodeStatuses = getNodeSnapshots();
        final List<NodeConnectionStatusSnapshotDTO> nodeStatusClones = new ArrayList<>(nodeStatuses.size());
        for (final NodeConnectionStatusSnapshotDTO nodeStatus : nodeStatuses) {
            nodeStatusClones.add(nodeStatus.clone());
        }
        other.setNodeSnapshots(nodeStatusClones);

        return other;
    }
}
