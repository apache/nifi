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

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@XmlType(name = "connectionStatistics")
public class ConnectionStatisticsDTO implements Cloneable {
    private String id;
    private Date statsLastRefreshed;

    private ConnectionStatisticsSnapshotDTO aggregateSnapshot;

    private List<NodeConnectionStatisticsSnapshotDTO> nodeSnapshots;

    @ApiModelProperty("The ID of the connection")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @ApiModelProperty("The status snapshot that represents the aggregate stats of the cluster")
    public ConnectionStatisticsSnapshotDTO getAggregateSnapshot() {
        return aggregateSnapshot;
    }

    public void setAggregateSnapshot(ConnectionStatisticsSnapshotDTO aggregateSnapshot) {
        this.aggregateSnapshot = aggregateSnapshot;
    }

    @ApiModelProperty("A list of status snapshots for each node")
    public List<NodeConnectionStatisticsSnapshotDTO> getNodeSnapshots() {
        return nodeSnapshots;
    }

    public void setNodeSnapshots(List<NodeConnectionStatisticsSnapshotDTO> nodeSnapshots) {
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
    public ConnectionStatisticsDTO clone() {
        final ConnectionStatisticsDTO other = new ConnectionStatisticsDTO();
        other.setId(getId());
        other.setAggregateSnapshot(getAggregateSnapshot().clone());


        final List<NodeConnectionStatisticsSnapshotDTO> nodeStatuses = getNodeSnapshots();
        final List<NodeConnectionStatisticsSnapshotDTO> nodeStatusClones = new ArrayList<>(nodeStatuses.size());
        for (final NodeConnectionStatisticsSnapshotDTO nodeStatus : nodeStatuses) {
            nodeStatusClones.add(nodeStatus.clone());
        }
        other.setNodeSnapshots(nodeStatusClones);

        return other;
    }
}
