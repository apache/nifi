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
        other.setId(getId());

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
