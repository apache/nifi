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

import java.util.stream.Stream;

import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.util.Tuple;

public interface StatusMetricExtractFunction {

    /**
     * Searches through status history to extract observations for a given metric
     * @param metric metric value that should be extracted
     * @param statusHistory StatusHistory object used to search metrics
     * @return a Tuple with extracted observations (features and targets/labels)
     */
    Tuple<Stream<Double[]>, Stream<Double>> extractMetric(String metric, StatusHistory statusHistory);
}
