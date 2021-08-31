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
package org.apache.nifi.parameter;

import java.util.Collection;
import java.util.Objects;

public abstract class AbstractParameterGroup<T> {

    private final String groupName;

    private final Collection<T> items;

    /**
     * Creates a named parameter group.
     * @param groupName The parameter group name
     * @param items A collection of grouped items
     */
    protected AbstractParameterGroup(final String groupName, final Collection<T> items) {
        this.groupName = Objects.requireNonNull(groupName, "Group name is required");
        this.items = items;
    }

    /**
     * @return The group name
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * @return The collection of grouped items
     */
    public Collection<T> getItems() {
        return items;
    }
}
