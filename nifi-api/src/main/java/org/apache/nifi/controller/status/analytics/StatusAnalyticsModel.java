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

import java.util.Map;
import java.util.stream.Stream;

public interface StatusAnalyticsModel {

    /**
     * Train model with provided observations (features, labels/targets)
     * @param features Stream of feature observation values
     * @param labels target observation values
     */
    void learn(Stream<Double[]> features, Stream<Double> labels);

    /**
     * Return a prediction given observation values
     * @param feature feature observation values values
     * @return prediction of target/label
     */
    Double predict(Double[] feature);

    /**
     * Predict a feature given a known target and known predictor values (if multiple predictors are included with model)
     * @param predictVariableIndex index of feature that we would like to predict (index should align with order provided in model learn method)
     * @param knownVariablesWithIndex a map of known predictor values with their indexes if available
     * @param label known target value
     * @return prediction for variable
     */
    Double predictVariable(Integer predictVariableIndex, Map<Integer,Double> knownVariablesWithIndex, Double label);

    /**
     * Indicate if model supports online learning (e.g. can learn new observation samples to create a model)
     * @return boolean indicating online learning support
     */
    Boolean supportsOnlineLearning();

    /**
     * Returns a map of scores relevant to model (e.g. rSquared, Confidence Intervals, etc.)
     * @return Map of score names with values
     */
    Map<String,Double> getScores();

    /**
     * Resets a model by clearing observations and other calculations
     */
    void clear();

}
