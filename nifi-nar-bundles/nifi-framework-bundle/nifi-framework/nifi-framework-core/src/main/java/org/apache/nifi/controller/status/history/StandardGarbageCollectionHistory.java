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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StandardGarbageCollectionHistory implements GarbageCollectionHistory {
    private final Map<String, List<GarbageCollectionStatus>> statusesByManagerName = new HashMap<>();

    @Override
    public Set<String> getMemoryManagerNames() {
        return statusesByManagerName.keySet();
    }

    @Override
    public List<GarbageCollectionStatus> getGarbageCollectionStatuses(final String memoryManagerName) {
        final List<GarbageCollectionStatus> statuses = statusesByManagerName.get(memoryManagerName);
        if (statuses == null) {
            return Collections.emptyList();
        }

        return Collections.unmodifiableList(statuses);
    }

    public void addGarbageCollectionStatus(final GarbageCollectionStatus status) {
        final String managerName = status.getMemoryManagerName();
        final List<GarbageCollectionStatus> statuses = statusesByManagerName.computeIfAbsent(managerName, key -> new ArrayList<>());
        statuses.add(status);
    }
}
