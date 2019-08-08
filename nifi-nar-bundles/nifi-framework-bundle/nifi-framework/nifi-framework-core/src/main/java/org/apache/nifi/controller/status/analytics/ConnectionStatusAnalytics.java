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
import java.util.Random;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.status.analytics.models.MultivariateStatusAnalyticsModel;
import org.apache.nifi.controller.status.analytics.models.OrdinaryLeastSquaresMSAM;
import org.apache.nifi.controller.status.analytics.models.VariateStatusAnalyticsModel;
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

import com.google.common.primitives.Doubles;

public class ConnectionStatusAnalytics implements StatusAnalytics {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionStatusAnalytics.class);
    private Map<String, Tuple<StatusAnalyticsModel, ExtractFunction>> modelMap;
    private QueryWindow queryWindow;
    private final ComponentStatusRepository componentStatusRepository;
    private final FlowFileEventRepository flowFileEventRepository;
    private final String connectionIdentifier;
    private final FlowManager flowManager;
    private final Boolean supportOnlineLearning;
    private Boolean extendWindow = false;
    private static double SCORE_THRESHOLD = .90;

    public ConnectionStatusAnalytics(ComponentStatusRepository componentStatusRepository, FlowManager flowManager, FlowFileEventRepository flowFileEventRepository, String connectionIdentifier,
                                     Boolean supportOnlineLearning) {
        this.componentStatusRepository = componentStatusRepository;
        this.flowManager = flowManager;
        this.flowFileEventRepository = flowFileEventRepository;
        this.connectionIdentifier = connectionIdentifier;
        this.supportOnlineLearning = supportOnlineLearning;
    }

    public void init() {

        LOG.debug("Initialize analytics connection id: {} ", connectionIdentifier);

        if (this.modelMap == null || this.modelMap.isEmpty()) {
            Tuple<StatusAnalyticsModel, ExtractFunction> countModelFunction = new Tuple<>(new OrdinaryLeastSquaresMSAM(), extract);
            Tuple<StatusAnalyticsModel, ExtractFunction> byteModelFunction = new Tuple<>(new OrdinaryLeastSquaresMSAM(), extract);
            this.modelMap = new HashMap<>();
            //TODO: Should change keys used here
            this.modelMap.put(ConnectionStatusDescriptor.QUEUED_COUNT.getField(), countModelFunction);
            this.modelMap.put(ConnectionStatusDescriptor.QUEUED_BYTES.getField(), byteModelFunction);
        }

        refresh();
    }

    public void refresh() {

        if (this.queryWindow == null) {
            //Set query window to fresh value
            this.queryWindow = new QueryWindow(System.currentTimeMillis() - getIntervalTimeMillis(), System.currentTimeMillis());
        } else if (supportOnlineLearning) {
            //Obtain latest observations when available, extend window if needed to obtain minimum observations
            this.queryWindow = new QueryWindow(extendWindow ? queryWindow.getStartTimeMillis() : queryWindow.getEndTimeMillis(), System.currentTimeMillis());

        }
        modelMap.forEach((metric, modelFunction) -> {

            StatusAnalyticsModel model = modelFunction.getKey();
            ExtractFunction extract = modelFunction.getValue();
            StatusHistory statusHistory = componentStatusRepository.getConnectionStatusHistory(connectionIdentifier, queryWindow.getStartDateTime(), queryWindow.getEndDateTime(), Integer.MAX_VALUE);
            Tuple<Stream<Double[]>, Stream<Double>> modelData = extract.extractMetric(metric, statusHistory);
            Double[][] features = modelData.getKey().toArray(size -> new Double[size][1]);
            Double[] values = modelData.getValue().toArray(size -> new Double[size]);

            if (ArrayUtils.isNotEmpty(features)) {
                try {
                    LOG.debug("Refreshing model with new data for connection id: {} ", connectionIdentifier);
                    model.learn(Stream.of(features), Stream.of(values));
                    extendWindow = false;
                } catch (Exception ex) {
                    LOG.debug("Exception encountered while training model for connection id {}: {}", connectionIdentifier, ex.getMessage());
                    extendWindow = true;
                }
            } else {
                extendWindow = true;
            }

        });
    }

    /**
     * Returns the predicted time (in milliseconds) when backpressure is expected to be applied to this connection, based on the total number of bytes in the queue.
     *
     * @return milliseconds until backpressure is predicted to occur, based on the total number of bytes in the queue.
     */
    public Long getTimeToBytesBackpressureMillis() {

        final MultivariateStatusAnalyticsModel bytesModel = (MultivariateStatusAnalyticsModel) modelMap.get(ConnectionStatusDescriptor.QUEUED_BYTES.getField()).getKey();
        FlowFileEvent flowFileEvent = getStatusReport();

        final Connection connection = getConnection();
        if (connection == null) {
            throw new NoSuchElementException("Connection with the following id cannot be found:" + connectionIdentifier + ". Model should be invalidated!");
        }
        final String backPressureDataSize = connection.getFlowFileQueue().getBackPressureDataSizeThreshold();
        final double backPressureBytes = DataUnit.parseDataSize(backPressureDataSize, DataUnit.B);

        if(validModel(bytesModel) && flowFileEvent != null) {
            List<Tuple<Integer, Double>> predictFeatures = new ArrayList<>();
            Double inOutRatio = (flowFileEvent.getContentSizeOut() / (double)flowFileEvent.getContentSizeIn());
            predictFeatures.add(new Tuple<>(1, inOutRatio));
            return getTimePrediction(bytesModel.predictVariable(0, predictFeatures, backPressureBytes), System.currentTimeMillis());
        }else{
            return -1L;
        }
    }

    /**
     * Returns the predicted time (in milliseconds) when backpressure is expected to be applied to this connection, based on the number of objects in the queue.
     *
     * @return milliseconds until backpressure is predicted to occur, based on the number of objects in the queue.
     */
    public Long getTimeToCountBackpressureMillis() {

        final MultivariateStatusAnalyticsModel countModel = (MultivariateStatusAnalyticsModel) modelMap.get(ConnectionStatusDescriptor.QUEUED_COUNT.getField()).getKey();
        FlowFileEvent flowFileEvent = getStatusReport();

        final Connection connection = getConnection();
        if (connection == null) {
            throw new NoSuchElementException("Connection with the following id cannot be found:" + connectionIdentifier + ". Model should be invalidated!");
        }

        final double backPressureCountThreshold = connection.getFlowFileQueue().getBackPressureObjectThreshold();

        if(validModel(countModel) && flowFileEvent != null) {
            List<Tuple<Integer, Double>> predictFeatures = new ArrayList<>();
            Double inOutRatio = (flowFileEvent.getFlowFilesOut() / (double)flowFileEvent.getFlowFilesIn());
            predictFeatures.add(new Tuple<>(1, inOutRatio));
            return getTimePrediction(countModel.predictVariable(0, predictFeatures, backPressureCountThreshold), System.currentTimeMillis());
        }else{
            return -1L;
        }
    }

    /**
     * Returns the predicted total number of bytes in the queue to occur at the next configured interval (5 mins in the future, e.g.).
     *
     * @return milliseconds until backpressure is predicted to occur, based on the total number of bytes in the queue.
     */

    public Long getNextIntervalBytes() {
        final VariateStatusAnalyticsModel bytesModel = (VariateStatusAnalyticsModel) modelMap.get(ConnectionStatusDescriptor.QUEUED_BYTES.getField()).getKey();
        FlowFileEvent flowFileEvent = getStatusReport();

        if(validModel(bytesModel) && flowFileEvent != null) {
            List<Double> predictFeatures = new ArrayList<>();
            Long nextInterval =  System.currentTimeMillis() + getIntervalTimeMillis();
            Double inOutRatio = flowFileEvent.getContentSizeOut() / (double)flowFileEvent.getContentSizeIn();
            predictFeatures.add(nextInterval.doubleValue());
            predictFeatures.add(inOutRatio);
            return  (bytesModel.predict(predictFeatures.toArray(new Double[2]))).longValue();
        }else{
            return -1L;
        }
    }

    /**
     * Returns the predicted number of objects in the queue to occur at the next configured interval (5 mins in the future, e.g.).
     *
     * @return milliseconds until backpressure is predicted to occur, based on the number of bytes in the queue.
     */

    public Long getNextIntervalCount() {
        final VariateStatusAnalyticsModel countModel = (VariateStatusAnalyticsModel) modelMap.get(ConnectionStatusDescriptor.QUEUED_COUNT.getField()).getKey();
        FlowFileEvent flowFileEvent = getStatusReport();

        if(validModel(countModel) && flowFileEvent != null) {
            List<Double> predictFeatures = new ArrayList<>();
            Long nextInterval =  System.currentTimeMillis() + getIntervalTimeMillis();
            Double inOutRatio = flowFileEvent.getFlowFilesOut()/ (double)flowFileEvent.getFlowFilesIn();
            predictFeatures.add(nextInterval.doubleValue());
            predictFeatures.add(inOutRatio);
            return (countModel.predict(predictFeatures.toArray(new Double[2]))).longValue();
        }else{
            return -1L;
        }

    }

    public Long getNextIntervalPercentageUseCount() {

        final Connection connection = getConnection();
        if (connection == null) {
            throw new NoSuchElementException("Connection with the following id cannot be found:" + connectionIdentifier + ". Model should be invalidated!");
        }
        final double backPressureCountThreshold = connection.getFlowFileQueue().getBackPressureObjectThreshold();
        final long nextIntervalCount = getNextIntervalCount();

        if (nextIntervalCount > -1L) {
            return Math.min(100, Math.round((nextIntervalCount / backPressureCountThreshold) * 100));
        } else {
            return -1L;
        }

    }

    public Long getNextIntervalPercentageUseBytes() {

        final Connection connection = getConnection();
        if (connection == null) {
            throw new NoSuchElementException("Connection with the following id cannot be found:" + connectionIdentifier + ". Model should be invalidated!");
        }
        final String backPressureDataSize = connection.getFlowFileQueue().getBackPressureDataSizeThreshold();
        final double backPressureBytes = DataUnit.parseDataSize(backPressureDataSize, DataUnit.B);
        final long nextIntervalBytes = getNextIntervalBytes();

        if (nextIntervalBytes > -1L) {
            return Math.min(100, Math.round((getNextIntervalBytes() / backPressureBytes) * 100));
        } else {
            return -1L;
        }
    }

    public Long getIntervalTimeMillis() {
        return 3L * 60 * 1000;
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

        predictions.forEach((key, value) -> {
            LOG.debug("Prediction model for connection id {}: {}={} ", connectionIdentifier, key, value);
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

    private FlowFileEvent getStatusReport(){
        RepositoryStatusReport statusReport =  flowFileEventRepository.reportTransferEvents(System.currentTimeMillis());
        return statusReport.getReportEntry(this.connectionIdentifier);
    }

    private interface ExtractFunction {
        Tuple<Stream<Double[]>, Stream<Double>> extractMetric(String metric, StatusHistory statusHistory);
    }

    private Long getTimePrediction(Double prediction, Long timeMillis) {

        if (Double.isNaN(prediction) || Double.isInfinite(prediction)) {
            return -1L;
        } else if (prediction < timeMillis) {
            return 0L;
        } else {
            return Math.max(0, Math.round(prediction) - timeMillis);
        }
    }

    private boolean validModel(VariateStatusAnalyticsModel model){

        Double rSquared = model.getRSquared();

        if (rSquared == null || (Doubles.isFinite(rSquared) && !Double.isNaN(rSquared) && rSquared < SCORE_THRESHOLD)) {
            if(supportOnlineLearning && model.supportsOnlineLearning()){
                model.clear();
            }
            return false;
        }else {
            return true;
        }
    }

    private final ExtractFunction extract = (metric, statusHistory) -> {

        List<Double> values = new ArrayList<>();
        List<Double[]> features = new ArrayList<>();
        Random rand = new Random();
        StatusHistoryDTO statusHistoryDTO = StatusHistoryUtil.createStatusHistoryDTO(statusHistory);

        for (StatusSnapshotDTO snap : statusHistoryDTO.getAggregateSnapshots()) {
            List<Double> featureArray = new ArrayList<>();
            Long snapValue = snap.getStatusMetrics().get(metric);
            long snapTime = snap.getTimestamp().getTime();

            featureArray.add((double) snapTime);
            Double randomError = + (rand.nextInt(1000) * .0000001);
            if (metric.equals(ConnectionStatusDescriptor.QUEUED_COUNT.getField())) {

                Long inputCount = snap.getStatusMetrics().get(ConnectionStatusDescriptor.INPUT_COUNT.getField());
                Long outputCount = snap.getStatusMetrics().get(ConnectionStatusDescriptor.OUTPUT_COUNT.getField());
                Double inOutRatio = ((double)outputCount /(double)inputCount) + randomError;
                featureArray.add(Double.isNaN(inOutRatio)? randomError : inOutRatio);

            } else {
                Long inputBytes = snap.getStatusMetrics().get(ConnectionStatusDescriptor.INPUT_BYTES.getField());
                Long outputBytes = snap.getStatusMetrics().get(ConnectionStatusDescriptor.OUTPUT_BYTES.getField());
                Double inOutRatio = ((double)outputBytes/(double)inputBytes) + randomError;
                featureArray.add(Double.isNaN(inOutRatio)? randomError : inOutRatio);
            }

            values.add((double) snapValue);
            features.add(featureArray.toArray(new Double[featureArray.size()]));

        }
        return new Tuple<Stream<Double[]>, Stream<Double>>(features.stream(), values.stream());

    };


}
