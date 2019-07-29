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

import javax.xml.bind.annotation.XmlType;

import io.swagger.annotations.ApiModelProperty;

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

    private Long predictedMillisUntilCountBackpressure = 0L;
    private Long predictedMillisUntilBytesBackpressure = 0L;
    private Integer predictedCountAtNextInterval = 0;
    private Long predictedBytesAtNextInterval = 0L;
    private Integer predictedPercentCount = 0;
    private Integer predictedPercentBytes = 0;
    private Long predictionIntervalMillis = 0L;

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

    @ApiModelProperty("The predicted number of milliseconds before the connection will have backpressure applied, based on the queued count.")
    public Long getPredictedMillisUntilCountBackpressure() {
        return predictedMillisUntilCountBackpressure;
    }

    public void setPredictedMillisUntilCountBackpressure(Long predictedMillisUntilCountBackpressure) {
        this.predictedMillisUntilCountBackpressure = predictedMillisUntilCountBackpressure;
    }

    @ApiModelProperty("The predicted number of milliseconds before the connection will have backpressure applied, based on the total number of bytes in the queue.")
    public Long getPredictedMillisUntilBytesBackpressure() {
        return predictedMillisUntilBytesBackpressure;
    }

    public void setPredictedMillisUntilBytesBackpressure(Long predictedMillisUntilBytesBackpressure) {
        this.predictedMillisUntilBytesBackpressure = predictedMillisUntilBytesBackpressure;
    }

    @ApiModelProperty("The predicted number of queued objects at the next configured interval.")
    public Integer getPredictedCountAtNextInterval() {
        return predictedCountAtNextInterval;
    }

    public void setPredictedCountAtNextInterval(Integer predictedCountAtNextInterval) {
        this.predictedCountAtNextInterval = predictedCountAtNextInterval;
    }

    @ApiModelProperty("The predicted total number of bytes in the queue at the next configured interval.")
    public Long getPredictedBytesAtNextInterval() {
        return predictedBytesAtNextInterval;
    }

    public void setPredictedBytesAtNextInterval(Long predictedBytesAtNextInterval) {
        this.predictedBytesAtNextInterval = predictedBytesAtNextInterval;
    }

    @ApiModelProperty("The predicted percentage of queued objects at the next configured interval.")
    public Integer getPredictedPercentCount() {
        return predictedPercentCount;
    }

    public void setPredictedPercentCount(Integer predictedPercentCount) {
        this.predictedPercentCount = predictedPercentCount;
    }

    @ApiModelProperty("The predicted percentage of bytes in the queue against current threshold at the next configured interval.")
    public Integer getPredictedPercentBytes() {
        return predictedPercentBytes;
    }

    public void setPredictedPercentBytes(Integer predictedPercentBytes) {
        this.predictedPercentBytes = predictedPercentBytes;
    }

    @ApiModelProperty("The prediction interval in seconds")
    public Long getPredictionIntervalMillis() {
        return predictionIntervalMillis;
    }

    public void setPredictionIntervalMillis(Long predictionIntervalMillis) {
        this.predictionIntervalMillis = predictionIntervalMillis;
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

        other.setPredictedMillisUntilCountBackpressure(getPredictedMillisUntilCountBackpressure());
        other.setPredictedMillisUntilBytesBackpressure(getPredictedMillisUntilBytesBackpressure());
        other.setPredictedCountAtNextInterval(getPredictedCountAtNextInterval());
        other.setPredictedBytesAtNextInterval(getPredictedBytesAtNextInterval());
        other.setPredictedPercentCount(getPredictedPercentCount());
        other.setPredictedPercentBytes(getPredictedPercentBytes());
        other.setPredictionIntervalMillis(getPredictionIntervalMillis());

        return other;
    }
}
