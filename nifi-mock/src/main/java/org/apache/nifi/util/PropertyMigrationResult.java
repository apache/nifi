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

package org.apache.nifi.util;

import org.apache.nifi.components.PropertyDescriptor;

import java.util.Map;
import java.util.Set;

public interface PropertyMigrationResult {

    /**
     * @return a set containing the names of all properties that were removed
     */
    Set<String> getPropertiesRemoved();

    /**
     * @return a mapping of previous property names to the new names of those properties
     */
    Map<String, String> getPropertiesRenamed();

    /**
     * @return a set of all controller services that were added
     */
    Set<MockPropertyConfiguration.CreatedControllerService> getCreatedControllerServices();

    /**
     * @return a set of all properties whose values were updated via calls to {@link org.apache.nifi.migration.PropertyConfiguration#setProperty(String, String)} or
     * {@link org.apache.nifi.migration.PropertyConfiguration#setProperty(PropertyDescriptor, String)}.
     */
    Set<String> getPropertiesUpdated();
}
