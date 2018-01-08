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

package org.apache.nifi.components;

import java.util.Optional;

public interface VersionedComponent {

    /**
     * @return the unique identifier that maps this component to a component that is versioned
     *         in a Flow Registry, or <code>Optional.empty</code> if this component has not been saved to a Flow Registry.
     */
    Optional<String> getVersionedComponentId();

    /**
     * Updates the versioned component identifier
     *
     * @param versionedComponentId the identifier of the versioned component
     *
     * @throws IllegalStateException if this component is already under version control with a different ID and
     *             the given ID is not null
     */
    void setVersionedComponentId(String versionedComponentId);
}
