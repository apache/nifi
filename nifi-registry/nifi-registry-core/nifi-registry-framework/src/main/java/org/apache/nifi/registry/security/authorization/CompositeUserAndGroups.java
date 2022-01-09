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
package org.apache.nifi.registry.security.authorization;

import java.util.HashSet;
import java.util.Set;

public class CompositeUserAndGroups implements UserAndGroups {

    private User user;
    private Set<Group> groups;

    public CompositeUserAndGroups() {
        this.user = null;
        this.groups = null;
    }

    public CompositeUserAndGroups(User user, Set<Group> groups) {
        this.user = user;
        setGroups(groups);
    }

    @Override
    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    @Override
    public Set<Group> getGroups() {
        return groups;
    }

    public void setGroups(Set<Group> groups) {
        // copy the collection so that if we add to this collection it does not modify other references
        if (groups != null) {
            this.groups = new HashSet<>(groups);
        } else {
            this.groups = null;
        }
    }

    public void addAllGroups(Set<Group> groups) {
        if (groups != null) {
            if (this.groups == null) {
                this.groups = new HashSet<>();
            }
            this.groups.addAll(groups);
        }
    }

    public void addGroup(Group group) {
        if (group != null) {
            if (this.groups == null) {
                this.groups = new HashSet<>();
            }
            this.groups.add(group);
        }
    }


}
