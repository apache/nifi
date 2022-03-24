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

import java.util.Map;
import java.util.stream.Stream;

import org.apache.nifi.controller.status.analytics.StatusAnalyticsModel;
/**
 * <p>
 * An abstract class for implementations of {@link StatusAnalyticsModel} which makes bivariate models compatible with analytics interface
 * </p>
 */
public abstract class BivariateStatusAnalyticsModel implements StatusAnalyticsModel {

    @Override
    public abstract void learn(Stream<Double[]> features, Stream<Double> labels);

    @Override
    public Double predict(Double[] feature){
        return predictY(feature[0]);
    }

    @Override
    public Double predictVariable(Integer predictVariableIndex, Map<Integer, Double> knownVariablesWithIndex, Double label) {
        return predictY(label);
    }

    @Override
    public Boolean supportsOnlineLearning() {
        return false;
    }

    public abstract Double predictX(Double y);

    public abstract Double predictY(Double x);

    @Override
    public abstract Map<String,Double> getScores();

    @Override
    public void clear() {
    }

}
