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

import java.util.Map;

/**
 * Stores and returns the details of a given component. Implementations are expected to be thread safe.
 */
public interface ComponentDetailsStorage {

    /**
     * Returns with the details of a given component if known. A component is know if it was present in the last call
     * of {@code #addComponentDetails}.
     *
     * @param componentId The component's identifier.
     *
     * @return A map of the details used for presenting status history if the component is known. Empty map otherwise.
     */
    Map<String, String> getDetails(String componentId);

    /**
     * Sets the component details for the storage. The call overwrites the previous values.
     *
     * @param componentDetails The known component details.
     */
    void setComponentDetails(Map<String, ComponentDetails> componentDetails);
}
