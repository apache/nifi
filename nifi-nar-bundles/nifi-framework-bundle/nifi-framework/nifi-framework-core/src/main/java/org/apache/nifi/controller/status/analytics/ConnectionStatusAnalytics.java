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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Doubles;
/**
 * <p>
 * An implementation of {@link StatusAnalytics} that is provides Connection related analysis/prediction for a given connection instance
 * </p>
 */
public class ConnectionStatusAnalytics implements StatusAnalytics {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionStatusAnalytics.class);
    private final Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap;
    private QueryWindow queryWindow;
    private final StatusHistoryRepository statusHistoryRepository;
    private final String connectionIdentifier;
    private final FlowManager flowManager;
    private final Boolean supportOnlineLearning;
    private Boolean extendWindow = false;
    private long intervalMillis = 3L * 60 * 1000; // Default is 3 minutes
    private long queryIntervalMillis = 5L * 60 * 1000;  //Default is 3 minutes
    private String scoreName = "rSquared";
    private double scoreThreshold = .90;
    private Map<String, Long> predictions;

    private static String TIME_TO_BYTE_BACKPRESSURE_MILLIS = "timeToBytesBackpressureMillis";
    private static String TIME_TO_COUNT_BACKPRESSURE_MILLIS = "timeToCountBackpressureMillis";
    private static String NEXT_INTERVAL_BYTES = "nextIntervalBytes";
    private static String NEXT_INTERVAL_COUNT = "nextIntervalCount";
    private static String NEXT_INTERVAL_PERCENTAGE_USE_COUNT = "nextIntervalPercentageUseCount";
    private static String NEXT_INTERVAL_PERCENTAGE_USE_BYTES = "nextIntervalPercentageUseBytes";
    private static String INTERVAL_TIME_MILLIS = "intervalTimeMillis";

    public ConnectionStatusAnalytics(StatusHistoryRepository statusHistoryRepository, FlowManager flowManager,
                                     Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap, String connectionIdentifier, Boolean supportOnlineLearning) {
        this.statusHistoryRepository = statusHistoryRepository;
        this.flowManager = flowManager;
        this.modelMap = modelMap;
        this.connectionIdentifier = connectionIdentifier;
        this.supportOnlineLearning = supportOnlineLearning;
        predictions = initPredictions();
    }

    /**
     *  Retrieve observations and train available model(s)
     */
    public void refresh() {

        if (supportOnlineLearning && this.queryWindow != null) {
            //Obtain latest observations when available, extend window if needed to obtain minimum observations
            this.queryWindow = new QueryWindow(extendWindow ? queryWindow.getStartTimeMillis() : queryWindow.getEndTimeMillis(), System.currentTimeMillis());
        } else {
            this.queryWindow = new QueryWindow(System.currentTimeMillis() - getQueryIntervalMillis(), System.currentTimeMillis());
        }

        modelMap.forEach((metric, modelFunction) -> {

            StatusAnalyticsModel model = modelFunction.getKey();
            StatusMetricExtractFunction extract = modelFunction.getValue();
            StatusHistory statusHistory = statusHistoryRepository.getConnectionStatusHistory(connectionIdentifier, queryWindow.getStartDateTime(), queryWindow.getEndDateTime(), Integer.MAX_VALUE);
            Tuple<Stream<Double[]>, Stream<Double>> modelData = extract.extractMetric(metric, statusHistory);
            Double[][] features = modelData.getKey().toArray(size -> new Double[size][1]);
            Double[] values = modelData.getValue().toArray(size -> new Double[size]);

            if (ArrayUtils.isNotEmpty(features)) {
                try {
                    LOG.debug("Refreshing model with new data for connection id: {} ", connectionIdentifier);
                    model.learn(Stream.of(features), Stream.of(values));

                    if(LOG.isDebugEnabled() && MapUtils.isNotEmpty(model.getScores())){
                        model.getScores().forEach((key, value) -> {
                            LOG.debug("Model Scores for prediction metric {} for connection id {}: {}={} ", metric, connectionIdentifier, key, value);
                        });
                    }

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

    protected StatusAnalyticsModel getModel(String modelType){

        if(modelMap.containsKey(modelType)){
            return modelMap.get(modelType).getKey();
        }else{
            throw new IllegalArgumentException("Model cannot be found for provided type: " + modelType);
        }
    }
    /**
     * Returns the predicted time (in milliseconds) when backpressure is expected to be applied to this connection, based on the total number of bytes in the queue.
     *
     * @return milliseconds until backpressure is predicted to occur, based on the total number of bytes in the queue.
     */
    Long getTimeToBytesBackpressureMillis(final Connection connection, FlowFileEvent flowFileEvent) {

        final StatusAnalyticsModel bytesModel = getModel("queuedBytes");
        final String backPressureDataSize = connection.getFlowFileQueue().getBackPressureDataSizeThreshold();
        final double backPressureBytes = DataUnit.parseDataSize(backPressureDataSize, DataUnit.B);

        if (validModel(bytesModel) && flowFileEvent != null) {
            Map<Integer, Double> predictFeatures = new HashMap<>();
            Double inOutRatio = (flowFileEvent.getContentSizeOut() / (double) flowFileEvent.getContentSizeIn());
            predictFeatures.put(1, inOutRatio);
            return convertTimePrediction(bytesModel.predictVariable(0, predictFeatures, backPressureBytes), System.currentTimeMillis());
        } else {
            LOG.debug("Model is not valid for calculating time back pressure by content size in bytes. Returning -1");
            return -1L;
        }
    }

    /**
     * Returns the predicted time (in milliseconds) when backpressure is expected to be applied to this connection, based on the number of objects in the queue.
     *
     * @return milliseconds until backpressure is predicted to occur, based on the number of objects in the queue.
     */
    Long getTimeToCountBackpressureMillis(final Connection connection, FlowFileEvent flowFileEvent) {

        final StatusAnalyticsModel countModel = getModel("queuedCount");

        final double backPressureCountThreshold = connection.getFlowFileQueue().getBackPressureObjectThreshold();

        if (validModel(countModel) && flowFileEvent != null) {
            Map<Integer, Double> predictFeatures = new HashMap<>();
            Double inOutRatio = (flowFileEvent.getFlowFilesOut() / (double) flowFileEvent.getFlowFilesIn());
            predictFeatures.put(1, inOutRatio);
            return convertTimePrediction(countModel.predictVariable(0, predictFeatures, backPressureCountThreshold), System.currentTimeMillis());
        } else {
            LOG.debug("Model is not valid for calculating time to back pressure by object count. Returning -1");
            return -1L;
        }
    }

    /**
     * Returns the predicted total number of bytes in the queue to occur at the next configured interval (5 mins in the future, e.g.).
     *
     * @return milliseconds until backpressure is predicted to occur, based on the total number of bytes in the queue.
     */

    Long getNextIntervalBytes(FlowFileEvent flowFileEvent) {
        final StatusAnalyticsModel bytesModel = getModel("queuedBytes");

        if (validModel(bytesModel) && flowFileEvent != null) {
            List<Double> predictFeatures = new ArrayList<>();
            Long nextInterval = System.currentTimeMillis() + getIntervalTimeMillis();
            Double inOutRatio = flowFileEvent.getContentSizeOut() / (double) flowFileEvent.getContentSizeIn();
            predictFeatures.add(nextInterval.doubleValue());
            predictFeatures.add(inOutRatio);
            return convertCountPrediction(bytesModel.predict(predictFeatures.toArray(new Double[2])));
        } else {
            LOG.debug("Model is not valid for predicting content size in bytes for next interval. Returning -1");
            return -1L;
        }
    }

    /**
     * Returns the predicted number of objects in the queue to occur at the next configured interval (5 mins in the future, e.g.).
     *
     * @return milliseconds until backpressure is predicted to occur, based on the number of bytes in the queue.
     */

    Long getNextIntervalCount(FlowFileEvent flowFileEvent) {
        final StatusAnalyticsModel countModel = getModel("queuedCount");

        if (validModel(countModel) && flowFileEvent != null) {
            List<Double> predictFeatures = new ArrayList<>();
            Long nextInterval = System.currentTimeMillis() + getIntervalTimeMillis();
            Double inOutRatio = flowFileEvent.getFlowFilesOut() / (double) flowFileEvent.getFlowFilesIn();
            predictFeatures.add(nextInterval.doubleValue());
            predictFeatures.add(inOutRatio);
            return convertCountPrediction(countModel.predict(predictFeatures.toArray(new Double[2])));
        } else {
            LOG.debug("Model is not valid for predicting object count for next interval. Returning -1");
            return -1L;
        }

    }

    /**
     * Returns the predicted object count percentage in queue when compared to threshold (maximum value allowed) at the next configured interval
     *
     * @return percentage of bytes used at next interval
     */
    Long getNextIntervalPercentageUseCount(final Connection connection, FlowFileEvent flowFileEvent) {

        final double backPressureCountThreshold = connection.getFlowFileQueue().getBackPressureObjectThreshold();
        final long nextIntervalCount = getNextIntervalCount(flowFileEvent);

        if (nextIntervalCount > -1L) {
            return Math.min(100, Math.round((nextIntervalCount / backPressureCountThreshold) * 100));
        } else {
            return -1L;
        }

    }

    /**
     * Returns the predicted bytes percentage in queue when compared to threshold (maximum value allowed) at the next configured interval
     *
     * @return percentage of bytes used at next interval
     */

    Long getNextIntervalPercentageUseBytes(final Connection connection, FlowFileEvent flowFileEvent) {

        final String backPressureDataSize = connection.getFlowFileQueue().getBackPressureDataSizeThreshold();
        final double backPressureBytes = DataUnit.parseDataSize(backPressureDataSize, DataUnit.B);
        final long nextIntervalBytes = getNextIntervalBytes(flowFileEvent);

        if (nextIntervalBytes > -1L) {
            return Math.min(100, Math.round((getNextIntervalBytes(flowFileEvent) / backPressureBytes) * 100));
        } else {
            return -1L;
        }
    }

    public Long getIntervalTimeMillis() {
        return intervalMillis;
    }

    public void setIntervalTimeMillis(long intervalTimeMillis) {
        this.intervalMillis = intervalTimeMillis;
    }

    public long getQueryIntervalMillis() {
        return queryIntervalMillis;
    }

    public void setQueryIntervalMillis(long queryIntervalMillis) {
        this.queryIntervalMillis = queryIntervalMillis;
    }

    public String getScoreName() {
        return scoreName;
    }

    public void setScoreName(String scoreName) {
        this.scoreName = scoreName;
    }

    public double getScoreThreshold() {
        return scoreThreshold;
    }

    public void setScoreThreshold(double scoreThreshold) {
        this.scoreThreshold = scoreThreshold;
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
        return predictions;
    }

    public void loadPredictions(final RepositoryStatusReport statusReport) {
        long startTs = System.currentTimeMillis();
        Connection connection = flowManager.getConnection(connectionIdentifier);
        if (connection == null) {
            throw new NoSuchElementException("Connection with the following id cannot be found:" + connectionIdentifier + ". Model should be invalidated!");
        }
        FlowFileEvent flowFileEvent = statusReport.getReportEntry(connectionIdentifier);
        predictions.put(TIME_TO_BYTE_BACKPRESSURE_MILLIS, getTimeToBytesBackpressureMillis(connection, flowFileEvent));
        predictions.put(TIME_TO_COUNT_BACKPRESSURE_MILLIS, getTimeToCountBackpressureMillis(connection, flowFileEvent));
        predictions.put(NEXT_INTERVAL_BYTES, getNextIntervalBytes(flowFileEvent));
        predictions.put(NEXT_INTERVAL_COUNT, getNextIntervalCount(flowFileEvent));
        predictions.put(NEXT_INTERVAL_PERCENTAGE_USE_COUNT, getNextIntervalPercentageUseCount(connection, flowFileEvent));
        predictions.put(NEXT_INTERVAL_PERCENTAGE_USE_BYTES, getNextIntervalPercentageUseBytes(connection, flowFileEvent));
        predictions.put(INTERVAL_TIME_MILLIS, getIntervalTimeMillis());
        long endTs = System.currentTimeMillis();
        LOG.debug("Prediction Calculations for connectionID {}: {}", connectionIdentifier, endTs - startTs);
        predictions.forEach((key, value) -> {
            LOG.trace("Prediction model for connection id {}: {}={} ", connectionIdentifier, key, value);
        });
    }

    @Override
    public boolean supportsOnlineLearning() {
        return supportOnlineLearning;
    }

    private Map<String, Long> initPredictions() {
        predictions = new ConcurrentHashMap<>();
        predictions.put(TIME_TO_BYTE_BACKPRESSURE_MILLIS, -1L);
        predictions.put(TIME_TO_COUNT_BACKPRESSURE_MILLIS, -1L);
        predictions.put(NEXT_INTERVAL_BYTES, -1L);
        predictions.put(NEXT_INTERVAL_COUNT, -1L);
        predictions.put(NEXT_INTERVAL_PERCENTAGE_USE_COUNT, -1L);
        predictions.put(NEXT_INTERVAL_PERCENTAGE_USE_BYTES, -1L);
        predictions.put(INTERVAL_TIME_MILLIS, -1L);
        return predictions;
    }

    /**
     * Convert time into valid prediction value (-1 indicates no prediction available).
     * Valid time translates to prediction equal to current time or in the future
     * @param prediction prediction value
     * @param timeMillis time in milliseconds
     * @return valid model boolean
     */
    private Long convertTimePrediction(Double prediction, Long timeMillis) {
        if (Double.isNaN(prediction) || Double.isInfinite(prediction) || prediction < timeMillis) {
            LOG.debug("Time prediction value is invalid: {}. Returning -1.",prediction);
            return -1L;
        } else {
            return Math.max(0, Math.round(prediction) - timeMillis);
        }
    }

    /**
     * Convert count into valid prediction value (-1 indicates no prediction available)
     * @param prediction prediction value
     * @return prediction prediction value converted into valid value for consumption
     */
    private Long convertCountPrediction(Double prediction) {
        if (Double.isNaN(prediction) || Double.isInfinite(prediction)) {
            LOG.debug("Count prediction value is invalid: {}. Returning -1.",prediction);
            return -1L;
        } else {
            return Math.max(0, Math.round(prediction));
        }
    }

    /**
     * Check if model is valid for prediction based on score
     * @param model StatusAnalyticsModel object
     * @return valid model boolean
     */

    private boolean validModel(StatusAnalyticsModel model) {

        Double score = getScore(model);

        if (score == null || (Doubles.isFinite(score) && !Double.isNaN(score) && score < scoreThreshold)) {
            if (supportOnlineLearning && model.supportsOnlineLearning()) {
                model.clear();
            }
            return false;
        } else {
            return true;
        }
    }

    /**
     * Get specific score from trained model
     * @param model StatusAnalyticsModel object
     * @return scoreValue
     */

    private Double getScore(StatusAnalyticsModel model) {
        if (model != null && model.getScores() != null) {
            return model.getScores().get(scoreName);
        } else {
            return null;
        }
    }

}
