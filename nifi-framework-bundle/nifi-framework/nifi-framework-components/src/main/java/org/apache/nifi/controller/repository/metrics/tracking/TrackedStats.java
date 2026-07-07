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

package org.apache.nifi.controller.repository.metrics.tracking;

import org.apache.nifi.controller.metrics.ComponentMetricContext;
import org.apache.nifi.controller.repository.metrics.PerformanceTracker;
import org.apache.nifi.controller.repository.metrics.ProcessSessionEventBuilder;
import org.apache.nifi.processor.ProcessSession;

public interface TrackedStats {

    /**
     * Ends the tracking of stats and returns a {@link ProcessSessionEventBuilder} populated with the collected stats.
     * A builder is returned, rather than a completed event, because callers frequently need to populate additional
     * fields (such as the number of invocations) before building the event.
     *
     * @param componentMetricContext the context of the component that the stats belong to
     * @return a builder populated with the stats collected during tracking
     */
    ProcessSessionEventBuilder end(ComponentMetricContext componentMetricContext);

    /**
     * Returns the PerformanceTracker associated with the TrackedStats so that it may be provided to
     * {@link ProcessSession} etc. to gather performance metrics that are relevant.
     *
     * @return the PerformanceTracker associated with the TrackedStats
     */
    PerformanceTracker getPerformanceTracker();

}
