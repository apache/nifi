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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link ComponentDetailsStorage} using in memory data structure.
 */
public class InMemoryComponentDetailsStorage implements ComponentDetailsStorage {
    private final AtomicReference<Map<String, ComponentDetails>> componentDetails = new AtomicReference<>(new HashMap<>());

    @Override
    public Map<String, String> getDetails(final String componentId) {
        final ComponentDetails componentDetails = this.componentDetails.get().get(componentId);
        return componentDetails == null ? Collections.emptyMap() : componentDetails.toMap();
    }

    @Override
    public void setComponentDetails(final Map<String, ComponentDetails> componentDetails) {
        this.componentDetails.set(componentDetails);
    }
}
