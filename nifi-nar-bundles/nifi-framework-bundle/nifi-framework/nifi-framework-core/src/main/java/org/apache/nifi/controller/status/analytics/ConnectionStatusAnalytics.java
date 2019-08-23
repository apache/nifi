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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.status.history.ComponentStatusRepository;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Doubles;

public class ConnectionStatusAnalytics implements StatusAnalytics {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionStatusAnalytics.class);
    private final Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap;
    private QueryWindow queryWindow;
    private final ComponentStatusRepository componentStatusRepository;
    private final FlowFileEventRepository flowFileEventRepository;
    private final String connectionIdentifier;
    private final FlowManager flowManager;
    private final Boolean supportOnlineLearning;
    private Boolean extendWindow = false;
    private long intervalMillis = 3L * 60 * 1000; // Default is 3 minutes
    private String scoreName = "rSquared";
    private double scoreThreshold = .90;

    public ConnectionStatusAnalytics(ComponentStatusRepository componentStatusRepository, FlowManager flowManager, FlowFileEventRepository flowFileEventRepository,
                                     Map<String, Tuple<StatusAnalyticsModel, StatusMetricExtractFunction>> modelMap, String connectionIdentifier, Boolean supportOnlineLearning) {
        this.componentStatusRepository = componentStatusRepository;
        this.flowManager = flowManager;
        this.flowFileEventRepository = flowFileEventRepository;
        this.modelMap = modelMap;
        this.connectionIdentifier = connectionIdentifier;
        this.supportOnlineLearning = supportOnlineLearning;
    }

    public void refresh() {

        if (supportOnlineLearning && this.queryWindow != null) {
            //Obtain latest observations when available, extend window if needed to obtain minimum observations
            this.queryWindow = new QueryWindow(extendWindow ? queryWindow.getStartTimeMillis() : queryWindow.getEndTimeMillis(), System.currentTimeMillis());
        } else {
            this.queryWindow = new QueryWindow(System.currentTimeMillis() - getIntervalTimeMillis(), System.currentTimeMillis());
        }

        modelMap.forEach((metric, modelFunction) -> {

            StatusAnalyticsModel model = modelFunction.getKey();
            StatusMetricExtractFunction extract = modelFunction.getValue();
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
    public Long getTimeToBytesBackpressureMillis() {

        final StatusAnalyticsModel bytesModel = getModel("queuedBytes");
        FlowFileEvent flowFileEvent = getStatusReport();

        final Connection connection = getConnection();
        if (connection == null) {
            throw new NoSuchElementException("Connection with the following id cannot be found:" + connectionIdentifier + ". Model should be invalidated!");
        }
        final String backPressureDataSize = connection.getFlowFileQueue().getBackPressureDataSizeThreshold();
        final double backPressureBytes = DataUnit.parseDataSize(backPressureDataSize, DataUnit.B);

        if (validModel(bytesModel) && flowFileEvent != null) {
            Map<Integer, Double> predictFeatures = new HashMap<>();
            Double inOutRatio = (flowFileEvent.getContentSizeOut() / (double) flowFileEvent.getContentSizeIn());
            predictFeatures.put(1, inOutRatio);
            return convertTimePrediction(bytesModel.predictVariable(0, predictFeatures, backPressureBytes), System.currentTimeMillis());
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

        final StatusAnalyticsModel countModel = getModel("queuedCount");
        FlowFileEvent flowFileEvent = getStatusReport();

        final Connection connection = getConnection();
        if (connection == null) {
            throw new NoSuchElementException("Connection with the following id cannot be found:" + connectionIdentifier + ". Model should be invalidated!");
        }

        final double backPressureCountThreshold = connection.getFlowFileQueue().getBackPressureObjectThreshold();

        if (validModel(countModel) && flowFileEvent != null) {
            Map<Integer, Double> predictFeatures = new HashMap<>();
            Double inOutRatio = (flowFileEvent.getFlowFilesOut() / (double) flowFileEvent.getFlowFilesIn());
            predictFeatures.put(1, inOutRatio);
            return convertTimePrediction(countModel.predictVariable(0, predictFeatures, backPressureCountThreshold), System.currentTimeMillis());
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
        final StatusAnalyticsModel bytesModel = getModel("queuedBytes");
        FlowFileEvent flowFileEvent = getStatusReport();

        if (validModel(bytesModel) && flowFileEvent != null) {
            List<Double> predictFeatures = new ArrayList<>();
            Long nextInterval = System.currentTimeMillis() + getIntervalTimeMillis();
            Double inOutRatio = flowFileEvent.getContentSizeOut() / (double) flowFileEvent.getContentSizeIn();
            predictFeatures.add(nextInterval.doubleValue());
            predictFeatures.add(inOutRatio);
            return convertCountPrediction(bytesModel.predict(predictFeatures.toArray(new Double[2])));
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
        final StatusAnalyticsModel countModel = getModel("queuedCount");
        FlowFileEvent flowFileEvent = getStatusReport();

        if (validModel(countModel) && flowFileEvent != null) {
            List<Double> predictFeatures = new ArrayList<>();
            Long nextInterval = System.currentTimeMillis() + getIntervalTimeMillis();
            Double inOutRatio = flowFileEvent.getFlowFilesOut() / (double) flowFileEvent.getFlowFilesIn();
            predictFeatures.add(nextInterval.doubleValue());
            predictFeatures.add(inOutRatio);
            return convertCountPrediction(countModel.predict(predictFeatures.toArray(new Double[2])));
        } else {
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
        return intervalMillis;
    }

    public void setIntervalTimeMillis(long intervalTimeMillis) {
        this.intervalMillis = intervalTimeMillis;
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

    private FlowFileEvent getStatusReport() {
        RepositoryStatusReport statusReport = flowFileEventRepository.reportTransferEvents(System.currentTimeMillis());
        return statusReport.getReportEntry(this.connectionIdentifier);
    }

    private Long convertTimePrediction(Double prediction, Long timeMillis) {
        if (Double.isNaN(prediction) || Double.isInfinite(prediction) || prediction < timeMillis) {
            return -1L;
        } else {
            return Math.max(0, Math.round(prediction) - timeMillis);
        }
    }

    private Long convertCountPrediction(Double prediction) {
        if (Double.isNaN(prediction) || Double.isInfinite(prediction) || prediction < 0) {
            return -1L;
        } else {
            return Math.max(0, Math.round(prediction));
        }
    }

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

    private Double getScore(StatusAnalyticsModel model) {
        if (model != null && model.getScores() != null) {
            return model.getScores().get(scoreName);
        } else {
            return null;
        }
    }

}
