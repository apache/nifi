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

package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.registry.flow.VersionedComponent;

public interface DifferenceDescriptor {
    /**
     * Describes a difference between two flows
     *
     * @param type the difference
     * @param componentA the component in "Flow A"
     * @param componentB the component in "Flow B"
     * @param fieldName the name of the field that changed, or <code>null</code> if the field name does not apply for the difference type
     * @param valueA the value being compared from "Flow A"
     * @param valueB the value being compared from "Flow B"
     * @return a human-readable description of how the flows differ
     */
    String describeDifference(DifferenceType type, String flowAName, String flowBName, VersionedComponent componentA, VersionedComponent componentB, String fieldName,
        Object valueA, Object valueB);
}
