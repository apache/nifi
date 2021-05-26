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
package org.apache.nifi.registry.authorization;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@ApiModel
public class User extends Tenant {

    private Set<Tenant> userGroups;

    public User() {}

    public User(String identifier, String identity) {
        super(identifier, identity);
    }

    @ApiModelProperty(
            value = "The groups to which the user belongs.",
            readOnly = true
    )
    public Set<Tenant> getUserGroups() {
        return userGroups;
    }

    public void setUserGroups(Set<Tenant> userGroups) {
        this.userGroups = userGroups;
    }

    public void addUserGroups(Collection<? extends Tenant> userGroups) {
        if (userGroups != null) {
            if (this.userGroups == null) {
                this.userGroups = new HashSet<>();
            }
            this.userGroups.addAll(userGroups);
        }
    }

}
