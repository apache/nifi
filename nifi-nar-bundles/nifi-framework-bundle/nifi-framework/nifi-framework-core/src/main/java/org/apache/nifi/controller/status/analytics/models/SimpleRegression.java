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
package org.apache.nifi.controller.status.analytics.models;


import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.regression.RegressionResults;
import org.apache.nifi.controller.status.analytics.StatusAnalyticsModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * An implementation of the {@link StatusAnalyticsModel} that uses the SimpleRegression class from math for computation of regression. It only supports Bivariates
 * for x,y (multiple predictors are not supported by SimpleRegression). Online learning is supported for collecting multiple samples at different points in time
 *
 * </p>
 */
public class SimpleRegression extends BivariateStatusAnalyticsModel {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleRegression.class);
    private final org.apache.commons.math3.stat.regression.SimpleRegression regression;
    private final Boolean supportOnlineLearning;
    private RegressionResults results;

    public SimpleRegression() {
        this(true);
    }

    public SimpleRegression(Boolean supportOnlineLearning) {
        this.regression = new org.apache.commons.math3.stat.regression.SimpleRegression();
        this.supportOnlineLearning = supportOnlineLearning;
    }

    @Override
    public void learn(Stream<Double[]> features, Stream<Double> labels) {
        double[] labelArray = ArrayUtils.toPrimitive(labels.toArray(Double[]::new));
        double[][] featuresMatrix = features.map(feature -> ArrayUtils.toPrimitive(feature)).toArray(double[][]::new);

        if (!supportOnlineLearning) {
            regression.clear();
        }

        regression.addObservations(featuresMatrix, labelArray);
        results = regression.regress();
        LOG.debug("Model is using equation: y = {}x + {}, with R-squared {}, RMSE {}", regression.getSlope(), regression.getIntercept(),
                                                                                       results.getRSquared(), Math.sqrt(results.getMeanSquareError()));

    }

    @Override
    public Double predictX(Double y) {
        return (y - regression.getIntercept()) / regression.getSlope();
    }

    @Override
    public Double predictY(Double x) {
        return regression.getSlope() * x + regression.getIntercept();
    }

    @Override
    public Map<String, Double> getScores() {
        if(results == null){
            return null;
        }else{
            Map<String,Double> scores = new HashMap<>();
            scores.put("rSquared",results.getRSquared());
            scores.put("adjustedRSquared",results.getAdjustedRSquared());
            scores.put("residualSumSquares",results.getErrorSumSquares());
            scores.put("meanSquareError",results.getMeanSquareError());
            return scores;
        }
    }

    @Override
    public void clear() {
        results = null;
        regression.clear();
    }

    @Override
    public Boolean supportsOnlineLearning() {
        return supportOnlineLearning;
    }

}
