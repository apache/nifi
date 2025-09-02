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

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.nifi.web.api.dto.util.TimeAdapter;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * DTO for serializing the status of a connector.
 */
@XmlType(name = "connectorStatus")
public class ConnectorStatusDTO implements Cloneable {

    private String id;
    private String groupId;
    private String name;
    private String type;
    private String runStatus;
    private String validationStatus;
    private Date statsLastRefreshed;

    private ConnectorStatusSnapshotDTO aggregateSnapshot;
    private List<NodeConnectorStatusSnapshotDTO> nodeSnapshots;

    /**
     * The id of the connector.
     *
     * @return The connector id
     */
    @Schema(description = "The id of the connector.")
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    /**
     * The id of the group this connector belongs to.
     *
     * @return The group id
     */
    @Schema(description = "The id of the group this connector belongs to.")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    /**
     * The name of the connector.
     *
     * @return The connector name
     */
    @Schema(description = "The name of the connector.")
    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    /**
     * The type of the connector.
     *
     * @return The connector type
     */
    @Schema(description = "The type of the connector.")
    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    /**
     * The run status of the connector.
     *
     * @return The run status
     */
    @Schema(description = "The run status of the connector.")
    public String getRunStatus() {
        return runStatus;
    }

    public void setRunStatus(final String runStatus) {
        this.runStatus = runStatus;
    }

    /**
     * The validation status of the connector.
     *
     * @return The validation status
     */
    @Schema(description = "The validation status of the connector.",
            allowableValues = {"VALID", "INVALID", "VALIDATING"})
    public String getValidationStatus() {
        return validationStatus;
    }

    public void setValidationStatus(final String validationStatus) {
        this.validationStatus = validationStatus;
    }

    @XmlJavaTypeAdapter(TimeAdapter.class)
    @Schema(description = "The timestamp of when the stats were last refreshed.", type = "string")
    public Date getStatsLastRefreshed() {
        return statsLastRefreshed;
    }

    public void setStatsLastRefreshed(final Date statsLastRefreshed) {
        this.statsLastRefreshed = statsLastRefreshed;
    }

    @Schema(description = "A status snapshot that represents the aggregate stats of all nodes in the cluster. If the NiFi instance is "
        + "a standalone instance, rather than a cluster, this represents the stats of the single instance.")
    public ConnectorStatusSnapshotDTO getAggregateSnapshot() {
        return aggregateSnapshot;
    }

    public void setAggregateSnapshot(final ConnectorStatusSnapshotDTO aggregateSnapshot) {
        this.aggregateSnapshot = aggregateSnapshot;
    }

    @Schema(description = "A status snapshot for each node in the cluster. If the NiFi instance is a standalone instance, rather than "
        + "a cluster, this may be null.")
    public List<NodeConnectorStatusSnapshotDTO> getNodeSnapshots() {
        return nodeSnapshots;
    }

    public void setNodeSnapshots(final List<NodeConnectorStatusSnapshotDTO> nodeSnapshots) {
        this.nodeSnapshots = nodeSnapshots;
    }

    @Override
    public ConnectorStatusDTO clone() {
        final ConnectorStatusDTO other = new ConnectorStatusDTO();
        other.setId(getId());
        other.setGroupId(getGroupId());
        other.setName(getName());
        other.setType(getType());
        other.setRunStatus(getRunStatus());
        other.setValidationStatus(getValidationStatus());
        other.setStatsLastRefreshed(getStatsLastRefreshed());

        if (getAggregateSnapshot() != null) {
            other.setAggregateSnapshot(getAggregateSnapshot().clone());
        }

        final List<NodeConnectorStatusSnapshotDTO> snapshots = getNodeSnapshots();
        if (snapshots != null) {
            final List<NodeConnectorStatusSnapshotDTO> snapshotClones = new ArrayList<>(snapshots.size());
            for (final NodeConnectorStatusSnapshotDTO snapshot : snapshots) {
                snapshotClones.add(snapshot.clone());
            }
            other.setNodeSnapshots(snapshotClones);
        }

        return other;
    }
}
