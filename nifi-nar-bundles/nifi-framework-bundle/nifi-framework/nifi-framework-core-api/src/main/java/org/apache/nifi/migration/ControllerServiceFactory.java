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

package org.apache.nifi.migration;

import org.apache.nifi.controller.service.ControllerServiceNode;

import java.util.Map;

public interface ControllerServiceFactory {

    /**
     * Determines whether or not a Controller Service exists in the proper scope with the given implementation and property values.
     * Provides all of the details that are necessary in order to create or reference the Controller Service with the given implementation
     * and property values
     *
     * @param implementationClassName the fully qualified classname of the Controller Service to create or reference
     * @param propertyValues          the property values that should be associated with the Controller Service
     * @return the details necessary in order to reference or create the Controller Service
     */
    ControllerServiceCreationDetails getCreationDetails(String implementationClassName, Map<String, String> propertyValues);

    /**
     * Creates a Controller Service that is described by the given details
     *
     * @param creationDetails the details of the service to create
     * @return the newly created Controller Service
     */
    ControllerServiceNode create(ControllerServiceCreationDetails creationDetails);
}
