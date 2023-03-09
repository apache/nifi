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

package org.apache.nifi.groups;

import org.apache.nifi.flow.VersionedExternalFlow;

public interface StatelessGroupFactory {
    /**
     * Creates a VersionedExternalFlow from the given ProcessGroup
     * @param group the ProcessGroup to create the VersionedExternalFlow from
     * @return the VersionedExternalFlow
     */
    VersionedExternalFlow createVersionedExternalFlow(ProcessGroup group);

    /**
     * Creates a ProcessGroup that can be used as the root group for a Stateless flow, based on the given VersionedExternalFlow and existing ProcessGroup.
     * The given group will then be set as the parent of the returned ProcessGroup, in order to facilitate the normal hierarchy of ProcessGroups, including access to
     * Controller Services that are defined at a higher level, etc.
     *
     * @param group the ProcessGroup
     * @param versionedExternalFlow the VersionedExternalFlow that was created from the given Process Group
     * @return the ProcessGroup that can be used as the root group for a Stateless flow
     */
    ProcessGroup createStatelessProcessGroup(ProcessGroup group, VersionedExternalFlow versionedExternalFlow);
}
