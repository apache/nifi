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
package org.apache.nifi.controller.status.history.storage;

import org.apache.nifi.controller.status.history.StatusHistory;

import java.time.Instant;

/**
 * Readable status storage for the specified component status entry type.
 *
 * @param <T> Stored component status entry type.
 */
public interface ComponentStatusStorage<T> extends StatusStorage<T> {

    /**
     * Select query template.
     */
    String QUERY_TEMPLATE =
            "SELECT * FROM %s " +
            "WHERE componentId = '%s' " +
            "AND capturedAt > to_timestamp('%s', '" + StatusStorage.CAPTURE_DATE_FORMAT + "') " +
            "AND capturedAt < to_timestamp('%s', '" + StatusStorage.CAPTURE_DATE_FORMAT + "') " +
            "ORDER BY capturedAt ASC";

    /**
     * Returns with the status history of the given component for the specified time range.
     *
     * @param componentId The component's unique id.
     * @param start Start date of the history, inclusive. In case it is not
     * specified, the history starts one day back from the current time.
     * @param end End date of the history, inclusive. In case it is not specified
     * the end date is the current time.
     * @param preferredDataPoints the preferred number of data points to return.
     * If the date range is large, the total number of data points could be far
     * too many to process. Therefore, this parameter allows the requestor to
     * indicate how many samples to return.
     *
     * @return Status history. In case the component does not exist, the result {@link StatusHistory} will be empty.
     */
    StatusHistory read(String componentId, Instant start, Instant end, int preferredDataPoints);
}
