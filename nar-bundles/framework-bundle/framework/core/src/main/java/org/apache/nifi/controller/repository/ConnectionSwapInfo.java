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
package org.apache.nifi.controller.repository;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.processor.QueueSize;

public class ConnectionSwapInfo {

    private final Map<String, Map<String, QueueSize>> connectionMap = new HashMap<>();

    public void addSwapSizeInfo(final String connectionId, final String swapFileLocation, final QueueSize queueSize) {
        Map<String, QueueSize> queueSizeMap = connectionMap.get(connectionId);
        if (queueSizeMap == null) {
            queueSizeMap = new HashMap<>();
            connectionMap.put(connectionId, queueSizeMap);
        }

        queueSizeMap.put(swapFileLocation, queueSize);
    }

    public Collection<String> getSwapFileLocations(final String connectionId) {
        final Map<String, QueueSize> sizeMap = connectionMap.get(connectionId);
        if (sizeMap == null) {
            return Collections.<String>emptyList();
        }

        return Collections.unmodifiableCollection(sizeMap.keySet());
    }

    public QueueSize getSwappedSize(final String connectionId, final String swapFileLocation) {
        final Map<String, QueueSize> sizeMap = connectionMap.get(connectionId);
        if (sizeMap == null) {
            return null;
        }

        return sizeMap.get(swapFileLocation);
    }

}
