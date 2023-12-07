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
package org.apache.nifi.controller.status.history.questdb;

final class QuestDbQueries {

    private QuestDbQueries() {
        // Should not be instantiated!
    }

    // Node status tables

    public static final String CREATE_NODE_STATUS =
            "CREATE TABLE nodeStatus (" +
                    "capturedAt TIMESTAMP," +
                    "freeHeap LONG," +
                    "usedHeap LONG," +
                    "heapUtilization LONG," +
                    "freeNonHeap LONG," +
                    "usedNonHeap LONG," +
                    "openFileHandlers LONG," +
                    "processorLoadAverage DOUBLE," +
                    "totalThreads LONG," +
                    "eventDrivenThreads LONG," +
                    "timerDrivenThreads LONG" +
            ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    public static final String CREATE_STORAGE_STATUS =
            "CREATE TABLE storageStatus (" +
                    "capturedAt TIMESTAMP," +
                    "name SYMBOL capacity 256 nocache," +
                    "storageType SHORT," +
                    "freeSpace LONG," +
                    "usedSpace LONG" +
            ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    public static final String CREATE_GARBAGE_COLLECTION_STATUS =
            "CREATE TABLE garbageCollectionStatus (" +
                    "capturedAt TIMESTAMP," +
                    "memoryManagerName SYMBOL capacity 4 nocache," +
                    "collectionCount LONG," +
                    "collectionMinis LONG" +
             ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    // Component status tables

    public static final String CREATE_PROCESSOR_STATUS =
            "CREATE TABLE processorStatus (" +
                    "capturedAt TIMESTAMP," +
                    "componentId SYMBOL capacity 2000 nocache index capacity 1500," +
                    "bytesRead LONG," +
                    "bytesWritten LONG," +
                    "bytesTransferred LONG," +
                    "inputBytes LONG," +
                    "inputCount LONG," +
                    "outputBytes LONG," +
                    "outputCount LONG," +
                    "taskCount LONG," +
                    "taskMillis LONG," +
                    "taskNanos LONG," +
                    "flowFilesRemoved LONG," +
                    "averageLineageDuration LONG," +
                    "averageTaskNanos LONG" +
            ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    public static final String CREATE_CONNECTION_STATUS =
            "CREATE TABLE connectionStatus (" +
                    "capturedAt TIMESTAMP," +
                    "componentId SYMBOL capacity 2000 nocache index capacity 1500," +
                    "inputBytes LONG," +
                    "inputCount LONG," +
                    "outputBytes LONG," +
                    "outputCount LONG," +
                    "queuedBytes LONG," +
                    "queuedCount LONG," +
                    "totalQueuedDuration LONG," +
                    "maxQueuedDuration LONG," +
                    "averageQueuedDuration LONG" +
            ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    public static final String CREATE_PROCESS_GROUP_STATUS =
            "CREATE TABLE processGroupStatus (" +
                    "capturedAt TIMESTAMP," +
                    "componentId SYMBOL capacity 2000 nocache index capacity 1500," +
                    "bytesRead LONG," +
                    "bytesWritten LONG," +
                    "bytesTransferred LONG," +
                    "inputBytes LONG," +
                    "inputCount LONG," +
                    "outputBytes LONG," +
                    "outputCount LONG," +
                    "queuedBytes LONG," +
                    "queuedCount LONG," +
                    "taskMillis LONG" +
                    ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    public static final String CREATE_REMOTE_PROCESS_GROUP_STATUS =
            "CREATE TABLE remoteProcessGroupStatus (" +
                    "capturedAt TIMESTAMP," +
                    "componentId SYMBOL capacity 2000 nocache index capacity 1500," +
                    "sentBytes LONG," +
                    "sentCount LONG," +
                    "receivedBytes LONG," +
                    "receivedCount LONG," +
                    "receivedBytesPerSecond LONG," +
                    "sentBytesPerSecond LONG," +
                    "totalBytesPerSecond LONG," +
                    "averageLineageDuration LONG" +
            ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    public static final String CREATE_COMPONENT_COUNTER =
            "CREATE TABLE componentCounter (" +
                    "capturedAt TIMESTAMP," +
                    "componentId SYMBOL capacity 2000 nocache index capacity 1500," +
                    "name SYMBOL capacity 256 nocache," +
                    "value LONG" +
            ") TIMESTAMP(capturedAt) PARTITION BY DAY";
}
