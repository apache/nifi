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
package org.apache.nifi.controller.service;

import java.util.Map;

import org.apache.nifi.controller.ControllerServiceLookup;

/**
 *
 */
public interface ControllerServiceProvider extends ControllerServiceLookup {

    /**
     * Gets the controller service for the specified identifier. Returns null if
     * the identifier does not match a known service.
     *
     * @param type
     * @param id
     * @param properties
     * @return
     */
    ControllerServiceNode createControllerService(String type, String id, Map<String, String> properties);

    /**
     * Gets the controller service node for the specified identifier. Returns
     * <code>null</code> if the identifier does not match a known service
     *
     * @param id
     * @return
     */
    ControllerServiceNode getControllerServiceNode(String id);
}
