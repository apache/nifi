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
 * @param <T>
 */
public interface MetricDescriptor<T> {

    public enum Formatter {

        COUNT,
        DURATION,
        DATA_SIZE
    };

    /**
     * Specifies how the values should be formatted
     *
     * @return
     */
    Formatter getFormatter();

    /**
     * Returns a human-readable description of the field
     *
     * @return
     */
    String getDescription();

    /**
     * Returns a human-readable label for the field
     *
     * @return
     */
    String getLabel();

    /**
     * Returns the name of a field
     *
     * @return
     */
    String getField();

    /**
     * Returns a {@link ValueMapper} that can be used to extract a value for the
     * status history
     *
     * @return
     */
    ValueMapper<T> getValueFunction();

    /**
     * Returns a {@link ValueReducer} that can reduce multiple StatusSnapshots
     * into a single Long value
     *
     * @return
     */
    ValueReducer<StatusSnapshot, Long> getValueReducer();
}
