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
package org.apache.nifi.web.api.dto;

import com.wordnik.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.entity.AccessPolicySummaryEntity;
import org.apache.nifi.web.api.entity.TenantEntity;

import javax.xml.bind.annotation.XmlType;
import java.util.Set;

/**
 * A user of this NiFi.
 */
@XmlType(name = "user")
public class UserDTO extends TenantDTO {

    private Set<TenantEntity> userGroups;
    private Set<AccessPolicySummaryEntity> accessPolicies;

    /**
     * @return groups to which the user belongs
     */
    @ApiModelProperty(
            value = "The groups to which the user belongs. This field is read only and it provided for convenience.",
            readOnly = true
    )
    public Set<TenantEntity> getUserGroups() {
        return userGroups;
    }

    public void setUserGroups(Set<TenantEntity> userGroups) {
        this.userGroups = userGroups;
    }

    /**
     * @return policies this user is part of
     */
    @ApiModelProperty(
            value = "The access policies this user belongs to.",
            readOnly = true
    )
    public Set<AccessPolicySummaryEntity> getAccessPolicies() {
        return accessPolicies;
    }

    public void setAccessPolicies(Set<AccessPolicySummaryEntity> accessPolicies) {
        this.accessPolicies = accessPolicies;
    }
}
