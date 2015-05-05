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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.ResourceNotFoundException;

public abstract class ComponentDAO {

    /**
     * Returns whether the specified object is not null.
     *
     * @param <T> type
     * @param object object
     * @return true if the specified object is not null
     */
    protected <T> boolean isNotNull(T object) {
        return object != null;
    }

    /**
     * Returns whether any of the specified objects are not null.
     *
     * @param <T> type
     * @param objects objects
     * @return true if any of the specified objects are not null
     */
    protected <T> boolean isAnyNotNull(T... objects) {
        for (final T object : objects) {
            if (object != null) {
                return true;
            }
        }

        return false;
    }

    /**
     * Locates the specified ProcessGroup.
     *
     * @param flowController controller
     * @param groupId id
     * @return group
     */
    protected ProcessGroup locateProcessGroup(FlowController flowController, String groupId) {
        ProcessGroup group = flowController.getGroup(groupId);

        if (group == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate group with id '%s'.", groupId));
        }

        return group;
    }
}
