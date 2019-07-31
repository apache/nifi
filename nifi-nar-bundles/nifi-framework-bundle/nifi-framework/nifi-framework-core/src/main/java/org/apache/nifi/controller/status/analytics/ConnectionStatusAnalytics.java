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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.apache.nifi.controller.status.history.ConnectionStatusDescriptor;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.controller.status.history.StatusHistoryUtil;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.dto.status.StatusSnapshotDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionStatusAnalytics implements StatusAnalytics {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionStatusAnalytics.class);
    private Map<String, Tuple<StatusAnalyticsModel, ExtractFunction>> modelMap;
    private QueryWindow queryWindow;
    private final ComponentStatusRepository componentStatusRepository;
    private final String connectionIdentifier;
    private final FlowManager flowManager;
    private final Boolean supportOnlineLearning;

    public ConnectionStatusAnalytics(ComponentStatusRepository componentStatusRepository, FlowManager flowManager, String connectionIdentifier, Boolean supportOnlineLearning) {
        this.componentStatusRepository = componentStatusRepository;
        this.flowManager = flowManager;
        this.connectionIdentifier = connectionIdentifier;
        this.supportOnlineLearning = supportOnlineLearning;
    }

    public void init() {

        LOG.debug("Initialize analytics connection id: {} ", connectionIdentifier);

        if (this.modelMap == null || this.modelMap.isEmpty()) {
            Tuple<StatusAnalyticsModel, ExtractFunction> countModelFunction = new Tuple<>(new SimpleRegressionBSAM(!supportsOnlineLearning()), extract);
            Tuple<StatusAnalyticsModel, ExtractFunction> byteModelFunction = new Tuple<>(new SimpleRegressionBSAM(!supportsOnlineLearning()), extract);
            this.modelMap = new HashMap<>();
            //TODO: Should change keys used here
            this.modelMap.put(ConnectionStatusDescriptor.QUEUED_COUNT.getField(), countModelFunction);
            this.modelMap.put(ConnectionStatusDescriptor.QUEUED_BYTES.getField(), byteModelFunction);
        }

        refresh();
    }

    public void refresh() {


        LOG.debug("Refreshing model with new data for connection id: {} ", connectionIdentifier);

        this.queryWindow = new QueryWindow(System.currentTimeMillis() - getIntervalTimeMillis(), System.currentTimeMillis());
        modelMap.forEach((metric, modelFunction) -> {

            StatusAnalyticsModel model = modelFunction.getKey();
            ExtractFunction extract = modelFunction.getValue();
            StatusHistory statusHistory = componentStatusRepository.getConnectionStatusHistory(connectionIdentifier, queryWindow.getStartDateTime(), queryWindow.getEndDateTime(), Integer.MAX_VALUE);
            Tuple<Stream<Double>, Stream<Double>> modelData = extract.extractMetric(metric, statusHistory);
            Stream<Double> times = modelData.getKey();
            Stream<Double> values = modelData.getValue();
            //times are the X axis and values are on the y axis
            model.learn(times, values);

        });
    }

    /**
     * Returns the predicted time (in milliseconds) when backpressure is expected to be applied to this connection, based on the total number of bytes in the queue.
     *
     * @return milliseconds until backpressure is predicted to occur, based on the total number of bytes in the queue.
     */
    public Long getTimeToBytesBackpressureMillis() {

        final BivariateStatusAnalyticsModel bytesModel = (BivariateStatusAnalyticsModel) modelMap.get(ConnectionStatusDescriptor.QUEUED_BYTES.getField()).getKey();
        final Connection connection = getConnection();
        if (connection == null) {
            throw new NoSuchElementException("Connection with the following id cannot be found:" + connectionIdentifier + ". Model should be invalidated!");
        }
        final String backPressureDataSize = connection.getFlowFileQueue().getBackPressureDataSizeThreshold();
        final double backPressureBytes = DataUnit.parseDataSize(backPressureDataSize, DataUnit.B);
        final double prediction = bytesModel.predictX(backPressureBytes);
        if (prediction != Double.NaN && prediction >= System.currentTimeMillis() && !Double.isInfinite(prediction)) {
            return Math.max(0, Math.round(prediction) - System.currentTimeMillis());
        } else {
            return -1L;
        }

    }

    /**
     * Returns the predicted time (in milliseconds) when backpressure is expected to be applied to this connection, based on the number of objects in the queue.
     *
     * @return milliseconds until backpressure is predicted to occur, based on the number of objects in the queue.
     */
    public Long getTimeToCountBackpressureMillis() {

        final BivariateStatusAnalyticsModel countModel = (BivariateStatusAnalyticsModel) modelMap.get(ConnectionStatusDescriptor.QUEUED_COUNT.getField()).getKey();
        final Connection connection = getConnection();
        if (connection == null) {
            throw new NoSuchElementException("Connection with the following id cannot be found:" + connectionIdentifier + ". Model should be invalidated!");
        }
        final double backPressureCountThreshold = connection.getFlowFileQueue().getBackPressureObjectThreshold();
        final Double prediction = countModel.predictX(backPressureCountThreshold);

        if (prediction != Double.NaN && prediction >= System.currentTimeMillis() && !Double.isInfinite(prediction)) {
            return Math.max(0, Math.round(prediction) - System.currentTimeMillis());
        } else {
            return -1L;
        }
    }

    /**
     * Returns the predicted total number of bytes in the queue to occur at the next configured interval (5 mins in the future, e.g.).
     *
     * @return milliseconds until backpressure is predicted to occur, based on the total number of bytes in the queue.
     */

    public Long getNextIntervalBytes() {
        final BivariateStatusAnalyticsModel bytesModel = (BivariateStatusAnalyticsModel) modelMap.get(ConnectionStatusDescriptor.QUEUED_BYTES.getField()).getKey();
        final Double prediction = bytesModel.predictY((double) System.currentTimeMillis() + getIntervalTimeMillis());
        if (prediction != Double.NaN && prediction >= 0) {
            return Math.round(prediction);
        } else {
            return -1L;
        }
    }

    /**
     * Returns the predicted number of objects in the queue to occur at the next configured interval (5 mins in the future, e.g.).
     *
     * @return milliseconds until backpressure is predicted to occur, based on the number of bytes in the queue.
     */

    public Long getNextIntervalCount() {
        final BivariateStatusAnalyticsModel countModel = (BivariateStatusAnalyticsModel) modelMap.get(ConnectionStatusDescriptor.QUEUED_COUNT.getField()).getKey();
        final Double prediction = countModel.predictY((double) System.currentTimeMillis() + getIntervalTimeMillis());
        if (prediction != Double.NaN && prediction >= 0) {
            return Math.round(prediction);
        } else {
            return -1L;
        }
    }

    public Long getNextIntervalPercentageUseCount(){

        final Connection connection = getConnection();
        if (connection == null) {
            throw new NoSuchElementException("Connection with the following id cannot be found:" + connectionIdentifier + ". Model should be invalidated!");
        }
        final double backPressureCountThreshold = connection.getFlowFileQueue().getBackPressureObjectThreshold();
        final long nextIntervalCount = getNextIntervalCount();

        if(nextIntervalCount > -1L) {
            return Math.round((getNextIntervalCount() / backPressureCountThreshold) * 100);
        }else{
            return -1L;
        }

    }

    public Long getNextIntervalPercentageUseBytes(){

        final Connection connection = getConnection();
        if (connection == null) {
            throw new NoSuchElementException("Connection with the following id cannot be found:" + connectionIdentifier + ". Model should be invalidated!");
        }
        final String backPressureDataSize = connection.getFlowFileQueue().getBackPressureDataSizeThreshold();
        final double backPressureBytes = DataUnit.parseDataSize(backPressureDataSize, DataUnit.B);
        final long nextIntervalBytes = getNextIntervalBytes();

        if(nextIntervalBytes > -1L) {
            return Math.round((getNextIntervalBytes() / backPressureBytes) * 100);
        }else{
            return -1L;
        }
    }

    public Long getIntervalTimeMillis(){
        return (5L * 60 * 1000);
    }

    @Override
    public QueryWindow getQueryWindow() {
        return queryWindow;
    }

    /**
     * Returns all available predictions
     */
    @Override
    public Map<String, Long> getPredictions() {

        Map<String, Long> predictions = new HashMap<>();
        predictions.put("timeToBytesBackpressureMillis", getTimeToBytesBackpressureMillis());
        predictions.put("timeToCountBackpressureMillis", getTimeToCountBackpressureMillis());
        predictions.put("nextIntervalBytes", getNextIntervalBytes());
        predictions.put("nextIntervalCount", getNextIntervalCount());
        predictions.put("nextIntervalPercentageUseCount", getNextIntervalPercentageUseCount());
        predictions.put("nextIntervalPercentageUseBytes", getNextIntervalPercentageUseBytes());
        predictions.put("intervalTimeMillis", getIntervalTimeMillis());

        predictions.forEach((key,value) -> {
            LOG.debug("Prediction model for connection id {}: {}={} ", connectionIdentifier,key,value);
        });

        return predictions;
    }

    @Override
    public boolean supportsOnlineLearning() {
        return supportOnlineLearning;
    }

    private Connection getConnection() {
        final ProcessGroup rootGroup = flowManager.getRootGroup();
        Optional<Connection> connection = rootGroup.findAllConnections().stream().filter(c -> c.getIdentifier().equals(this.connectionIdentifier)).findFirst();
        return connection.orElse(null);
    }

    private interface ExtractFunction {
        Tuple<Stream<Double>, Stream<Double>> extractMetric(String metric, StatusHistory statusHistory);
    }

    private final ExtractFunction extract = (metric, statusHistory) -> {

        List<Double> values = new ArrayList<>();
        List<Double> times = new ArrayList<>();

        StatusHistoryDTO statusHistoryDTO = StatusHistoryUtil.createStatusHistoryDTO(statusHistory);

        for (StatusSnapshotDTO snap : statusHistoryDTO.getAggregateSnapshots()) {
            Long snapValue = snap.getStatusMetrics().get(metric);
            long snapTime = snap.getTimestamp().getTime();
            values.add((double) snapValue);
            times.add((double) snapTime);
        }
        return new Tuple<>(times.stream(), values.stream());

    };


}
