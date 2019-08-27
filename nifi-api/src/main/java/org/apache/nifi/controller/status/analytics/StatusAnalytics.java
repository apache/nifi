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

/**
 * The StatusAnalytics interface offers methods for accessing predicted and other values for a single component (Connection instance, e.g.).
 */
public interface StatusAnalytics {

    /**
     * Get the Query Window used by the analytics instance
     * @return queryWindow
     */
    QueryWindow getQueryWindow();

    /**
     * Get available predictions where the key (String) in the map is the name of the prediction and value (Long)
     * is the value for the prediction
     * @return map
     */
    Map<String,Long> getPredictions();

    /**
     * Return if analytics object supports online learning
     * @return boolean
     */
    boolean supportsOnlineLearning();


}
