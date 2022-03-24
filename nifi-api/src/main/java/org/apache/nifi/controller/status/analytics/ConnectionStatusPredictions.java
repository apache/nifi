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
package org.apache.nifi.controller.status.analytics;

/**
 */
public class ConnectionStatusPredictions implements Cloneable {
    private long predictionIntervalMillis;
    private int nextPredictedQueuedCount;
    private long nextPredictedQueuedBytes;
    private long predictedTimeToCountBackpressureMillis;
    private long predictedTimeToBytesBackpressureMillis;
    private int predictedPercentCount = 0;
    private int predictedPercentBytes = 0;

    public long getPredictionIntervalMillis() {
        return predictionIntervalMillis;
    }

    public void setPredictionIntervalMillis(long predictionIntervalMillis) {
        this.predictionIntervalMillis = predictionIntervalMillis;
    }

    public int getNextPredictedQueuedCount() {
        return nextPredictedQueuedCount;
    }

    public void setNextPredictedQueuedCount(int nextPredictedQueuedCount) {
        this.nextPredictedQueuedCount = nextPredictedQueuedCount;
    }

    public long getNextPredictedQueuedBytes() {
        return nextPredictedQueuedBytes;
    }

    public void setNextPredictedQueuedBytes(long nextPredictedQueuedBytes) {
        this.nextPredictedQueuedBytes = nextPredictedQueuedBytes;
    }

    public long getPredictedTimeToCountBackpressureMillis() {
        return predictedTimeToCountBackpressureMillis;
    }

    public void setPredictedTimeToCountBackpressureMillis(long predictedTimeToCountBackpressureMillis) {
        this.predictedTimeToCountBackpressureMillis = predictedTimeToCountBackpressureMillis;
    }

    public long getPredictedTimeToBytesBackpressureMillis() {
        return predictedTimeToBytesBackpressureMillis;
    }

    public void setPredictedTimeToBytesBackpressureMillis(long predictedTimeToBytesBackpressureMillis) {
        this.predictedTimeToBytesBackpressureMillis = predictedTimeToBytesBackpressureMillis;
    }

    public int getPredictedPercentCount() {
        return predictedPercentCount;
    }

    public void setPredictedPercentCount(int predictedPercentCount) {
        this.predictedPercentCount = predictedPercentCount;
    }

    public int getPredictedPercentBytes() {
        return predictedPercentBytes;
    }

    public void setPredictedPercentBytes(int predictedPercentBytes) {
        this.predictedPercentBytes = predictedPercentBytes;
    }

    @Override
    public ConnectionStatusPredictions clone() {
        final ConnectionStatusPredictions clonedObj = new ConnectionStatusPredictions();
        clonedObj.nextPredictedQueuedBytes = nextPredictedQueuedBytes;
        clonedObj.nextPredictedQueuedCount = nextPredictedQueuedCount;
        clonedObj.predictedTimeToBytesBackpressureMillis = predictedTimeToBytesBackpressureMillis;
        clonedObj.predictedTimeToCountBackpressureMillis = predictedTimeToCountBackpressureMillis;
        clonedObj.predictedPercentCount = predictedPercentCount;
        clonedObj.predictedPercentBytes = predictedPercentBytes;
        return clonedObj;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ConnectionStatusPredictions [id=");
        builder.append(", nextPredictedQueuedBytes=");
        builder.append(nextPredictedQueuedBytes);
        builder.append(", nextPredictedQueuedCount=");
        builder.append(nextPredictedQueuedCount);
        builder.append(", predictedTimeToBytesBackpressureMillis=");
        builder.append(predictedTimeToBytesBackpressureMillis);
        builder.append(", predictedTimeToCountBackpressureMillis=");
        builder.append(predictedTimeToCountBackpressureMillis);
        builder.append(", predictedPercentCount=");
        builder.append(predictedPercentCount);
        builder.append(", predictedPercentBytes=");
        builder.append(predictedPercentBytes);
        builder.append("]");
        return builder.toString();
    }
}
