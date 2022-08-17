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

package org.apache.nifi.stateless.flow;

import org.apache.nifi.processor.DataUnit;

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * When a dataflow is triggered, depending on the components within the flow and their configuration,
 * some components may require that the source be triggered multiple times in order to provide it enough
 * data to perform its function. However, this situation can also lead to resource exhaustion by continually
 * triggering the data source to run when such a processor is greedily demanding too much data.
 * </p>
 *
 * <p>
 * Transaction Thresholds can be provided such that if a component needs more data to progress, the source will
 * be triggered only until the source of data has reached the provided thresholds. At that point, the data sources
 * will no longer be triggered, and it will be up to the greedy component to either complete its task with the data
 * that it has or to simply make no progress. Often, this is done up to some configured amount of time, after which
 * the component will simply timeout and perform its function with the data that it has.
 * </p>
 */
public interface TransactionThresholds {
    /**
     * @return the maximum number of FlowFiles that the source component should ingest
     */
    OptionalLong getMaxFlowFiles();

    /**
     * @return the maximum number of bytes (of FlowFile content) that the source component should ingest
     */
    OptionalLong getMaxContentSize(DataUnit dataUnit);

    /**
     * Indicates the maximum amount of time since the data flow was triggered before the source component should no longer be triggered
     * @param timeUnit the unit for the time period
     * @return The maximum amount of time since the data flow was triggered before the source component should no longer be triggered
     */
    OptionalLong getMaxTime(TimeUnit timeUnit);

    /**
     * A TransactionThresholds that limit the number of FlowFiles to 1 and place no bounds on the size of the contents or amount of time
     */
    TransactionThresholds SINGLE_FLOWFILE = new TransactionThresholds() {
        @Override
        public OptionalLong getMaxFlowFiles() {
            return OptionalLong.of(1L);
        }

        @Override
        public OptionalLong getMaxContentSize(final DataUnit dataUnit) {
            return OptionalLong.empty();
        }

        @Override
        public OptionalLong getMaxTime(final TimeUnit timeUnit) {
            return OptionalLong.empty();
        }

        @Override
        public String toString() {
            return "TransactionThresholds[maxFlowFiles=1, maxContentSize=unlimited, maxElapsedTime=unlimited]";
        }
    };
}
