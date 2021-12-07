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

package org.apache.nifi.stateless;

import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.stateless.flow.TransactionIngestStrategy;
import org.apache.nifi.stateless.flow.TransactionThresholds;

import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

public class TransactionThresholdsFactory {

    public static TransactionThresholds createTransactionThresholds(Integer maxFlowFiles) {
        return createTransactionThresholds(maxFlowFiles, TransactionIngestStrategy.LAZY);
    }

    public static TransactionThresholds createTransactionThresholds(Integer maxFlowFiles, TransactionIngestStrategy ingestStrategy) {
        return createTransactionThresholds(maxFlowFiles, null, ingestStrategy);
    }

    public static TransactionThresholds createTransactionThresholds(Integer maxFlowFiles, Integer maxBytes, TransactionIngestStrategy ingestStrategy) {
        return new TransactionThresholds() {
            @Override
            public OptionalLong getMaxFlowFiles() {
                return maxFlowFiles != null ? OptionalLong.of(maxFlowFiles) : OptionalLong.empty();
            }

            @Override
            public OptionalLong getMaxContentSize(final DataUnit dataUnit) {
                return maxBytes != null ? OptionalLong.of((long) dataUnit.convert(maxBytes, DataUnit.B)) : OptionalLong.empty();
            }

            @Override
            public OptionalLong getMaxTime(final TimeUnit timeUnit) {
                return OptionalLong.empty();
            }

            @Override
            public TransactionIngestStrategy getIngestStrategy() {
                return ingestStrategy;
            }
        };
    }
}
