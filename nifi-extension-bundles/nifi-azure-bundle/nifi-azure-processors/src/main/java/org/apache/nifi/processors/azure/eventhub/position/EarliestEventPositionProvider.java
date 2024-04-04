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
package org.apache.nifi.processors.azure.eventhub.position;

import com.azure.messaging.eventhubs.models.EventPosition;

import java.util.HashMap;
import java.util.Map;

/**
 * Earliest Event Position provides a workaround for implementing a start position other than EventPosition.latest()
 * The initial number of partitions is not known. This should be replaced pending implementation of
 * <a href="https://github.com/Azure/azure-sdk-for-java/issues/11431">Azure SDK for Java Issue 11431</a>
 */
public class EarliestEventPositionProvider implements EventPositionProvider {
    /**
     * Get Initial Partition Event Position using earliest available strategy
     *
     * @return Map of Partition to earliest Event Position
     */
    @Override
    public Map<String, EventPosition> getInitialPartitionEventPosition() {
        return new EarliestEventPosition();
    }

    private static class EarliestEventPosition extends HashMap<String, EventPosition> {
        /**
         * Contains Key returns true in order for PartitionPumpManager to request the EventPosition
         *
         * @param partitionId Partition Identifier requested
         * @return Returns true for all invocations
         */
        @Override
        public boolean containsKey(final Object partitionId) {
            return true;
        }

        /**
         * Get EventPosition.earliest() for PartitionPumpManager.startPartitionPump()
         *
         * @param partitionId Partition Identifier requested
         * @return EventPosition.earliest()
         */
        @Override
        public EventPosition get(final Object partitionId) {
            return EventPosition.earliest();
        }
    }
}
