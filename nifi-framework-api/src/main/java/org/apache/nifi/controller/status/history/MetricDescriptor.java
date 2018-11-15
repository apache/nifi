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
package org.apache.nifi.controller.status.history;

/**
 * Describes a particular metric that is derived from a Status History
 *
 * @param <T> type of metric
 */
public interface MetricDescriptor<T> {

    enum Formatter {
        COUNT,
        DURATION,
        DATA_SIZE
    };

    int getMetricIdentifier();

    /**
     * Specifies how the values should be formatted
     *
     * @return formatter for values
     */
    Formatter getFormatter();

    /**
     * @return a human-readable description of the field
     */
    String getDescription();

    /**
     * @return a human-readable label for the field
     */
    String getLabel();

    /**
     * @return the name of a field
     */
    String getField();

    /**
     * @return a {@link ValueMapper} that can be used to extract a value for the
     * status history
     */
    ValueMapper<T> getValueFunction();

    /**
     * @return a {@link ValueReducer} that can reduce multiple StatusSnapshots
     * into a single Long value
     */
    ValueReducer<StatusSnapshot, Long> getValueReducer();

    /**
     * @return <code>true</code> if the metric is for a component Counter, <code>false</code> otherwise
     */
    boolean isCounter();
}
