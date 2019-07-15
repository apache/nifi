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

import javax.xml.bind.annotation.XmlType;

/**
 * DTO for serializing the statistics of a connection.
 */
@XmlType(name = "connectionStatisticsSnapshot")
public class ConnectionStatisticsSnapshotDTO implements Cloneable {

    private String id;
    private String groupId;
    private String name;

    private String sourceId;
    private String sourceName;
    private String destinationId;
    private String destinationName;

    private Long predictedMillisUntilBackpressure = 0L;

    /* getters / setters */
    /**
     * @return The connection id
     */
    @ApiModelProperty("The id of the connection.")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the ID of the Process Group to which this connection belongs.
     */
    @ApiModelProperty("The id of the process group the connection belongs to.")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    /**
     * @return name of this connection
     */
    @ApiModelProperty("The name of the connection.")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return id of the source of this connection
     */
    @ApiModelProperty("The id of the source of the connection.")
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * @return name of the source of this connection
     */
    @ApiModelProperty("The name of the source of the connection.")
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    /**
     * @return id of the destination of this connection
     */
    @ApiModelProperty("The id of the destination of the connection.")
    public String getDestinationId() {
        return destinationId;
    }

    public void setDestinationId(String destinationId) {
        this.destinationId = destinationId;
    }

    /**
     * @return name of the destination of this connection
     */
    @ApiModelProperty("The name of the destination of the connection.")
    public String getDestinationName() {
        return destinationName;
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    @ApiModelProperty("The predicted number of milliseconds before the connection will have backpressure applied.")
    public Long getPredictedMillisUntilBackpressure() {
        return predictedMillisUntilBackpressure;
    }

    public void setPredictedMillisUntilBackpressure(Long predictedMillisUntilBackpressure) {
        this.predictedMillisUntilBackpressure = predictedMillisUntilBackpressure;
    }

    @Override
    public ConnectionStatisticsSnapshotDTO clone() {
        final ConnectionStatisticsSnapshotDTO other = new ConnectionStatisticsSnapshotDTO();
        other.setDestinationId(getDestinationId());
        other.setDestinationName(getDestinationName());
        other.setGroupId(getGroupId());
        other.setId(getId());
        other.setName(getName());
        other.setSourceId(getSourceId());
        other.setSourceName(getSourceName());

        other.setPredictedMillisUntilBackpressure(getPredictedMillisUntilBackpressure());

        return other;
    }
}
