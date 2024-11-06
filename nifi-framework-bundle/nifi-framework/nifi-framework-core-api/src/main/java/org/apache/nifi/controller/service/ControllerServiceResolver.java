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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.registry.flow.FlowSnapshotContainer;

import java.util.Set;

public interface ControllerServiceResolver {

    /**
     * Resolves controller service references in any processors or controller services that exist in the flow contents
     * of the top level snapshot provided in the flow snapshot container.
     *
     * The resolution looks for a service in the same group, or ancestor group, that provides the required API and has
     * the same name as referenced in the original snapshot. If only one such service exists, it is selected and the
     * value of the component's property descriptor is updated. If more than on service exists, the value of the
     * component's property descriptor is not modified.
     *
     * @param flowSnapshotContainer the container encapsulating the top level snapshot being imported, as well as the
     *                              snapshots of any child groups that are also under version control
     * @param parentGroupId the id of the process group where the snapshot is being imported
     * @param user the user performing the import
     * @return Any unresolved Controller Services
     */
    Set<String> resolveInheritedControllerServices(FlowSnapshotContainer flowSnapshotContainer, String parentGroupId, NiFiUser user);

}
