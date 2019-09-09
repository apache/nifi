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
 * DTO for serializing the status predictions of a connection.
 */
@XmlType(name = "connectionStatusSnapshot")
public class ConnectionStatusPredictionsSnapshotDTO implements Cloneable {
    private Long predictedMillisUntilCountBackpressure = 0L;
    private Long predictedMillisUntilBytesBackpressure = 0L;
    private Integer predictionIntervalSeconds;
    private Integer predictedCountAtNextInterval = 0;
    private Long predictedBytesAtNextInterval = 0L;
    private Integer predictedPercentCount;
    private Integer predictedPercentBytes;

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

    @ApiModelProperty("The configured interval (in seconds) for predicting connection queue count and size (and percent usage).")
    public Integer getPredictionIntervalSeconds() {
        return predictionIntervalSeconds;
    }

    public void setPredictionIntervalSeconds(Integer predictionIntervalSeconds) {
        this.predictionIntervalSeconds = predictionIntervalSeconds;
    }

    @ApiModelProperty("The predicted total number of bytes in the queue at the next configured interval.")
    public Long getPredictedBytesAtNextInterval() {
        return predictedBytesAtNextInterval;
    }

    public void setPredictedBytesAtNextInterval(Long predictedBytesAtNextInterval) {
        this.predictedBytesAtNextInterval = predictedBytesAtNextInterval;
    }

    @ApiModelProperty("Predicted connection percent use regarding queued flow files count and backpressure threshold if configured.")
    public Integer getPredictedPercentCount() {
        return predictedPercentCount;
    }

    public void setPredictedPercentCount(Integer predictedPercentCount) {
        this.predictedPercentCount = predictedPercentCount;
    }

    @ApiModelProperty("Predicted connection percent use regarding queued flow files size and backpressure threshold if configured.")
    public Integer getPredictedPercentBytes() {
        return predictedPercentBytes;
    }

    public void setPredictedPercentBytes(Integer predictedPercentBytes) {
        this.predictedPercentBytes = predictedPercentBytes;
    }

    @Override
    public ConnectionStatusPredictionsSnapshotDTO clone() {
        final ConnectionStatusPredictionsSnapshotDTO other = new ConnectionStatusPredictionsSnapshotDTO();
        other.setPredictedMillisUntilCountBackpressure(getPredictedMillisUntilCountBackpressure());
        other.setPredictedMillisUntilBytesBackpressure(getPredictedMillisUntilBytesBackpressure());
        other.setPredictionIntervalSeconds(getPredictionIntervalSeconds());
        other.setPredictedCountAtNextInterval(getPredictedCountAtNextInterval());
        other.setPredictedBytesAtNextInterval(getPredictedBytesAtNextInterval());
        other.setPredictedPercentBytes(getPredictedPercentBytes());
        other.setPredictedPercentCount(getPredictedPercentCount());

        return other;
    }
}
