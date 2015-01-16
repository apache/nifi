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

import java.util.Date;
import java.util.Map;

/**
 * A StatusSnapshot represents a Component's status report at some point in time
 */
public interface StatusSnapshot {

    /**
     * Rreturns the point in time for which the status values were obtained
     *
     * @return
     */
    Date getTimestamp();

    /**
     * Returns a Map of MetricDescriptor to value
     *
     * @return
     */
    Map<MetricDescriptor<?>, Long> getStatusMetrics();

    /**
     * Returns a {@link ValueReducer} that is capable of merging multiple
     * StatusSnapshot objects into a single one
     *
     * @return
     */
    ValueReducer<StatusSnapshot, StatusSnapshot> getValueReducer();
}
