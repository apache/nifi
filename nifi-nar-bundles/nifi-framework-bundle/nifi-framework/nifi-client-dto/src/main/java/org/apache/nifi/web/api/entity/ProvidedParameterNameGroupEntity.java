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
package org.apache.nifi.web.api.entity;

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;
import java.util.Set;

/**
 * Entity encapsulating parameter names for a given provided parameter group.
 */
@XmlType(name = "providedParameterNameGroup")
public class ProvidedParameterNameGroupEntity extends Entity implements Comparable<ProvidedParameterNameGroupEntity> {

    private String groupName;
    private String sensitivity;
    private Set<String> parameterNames;

    @ApiModelProperty(
            value = "The name of the external parameter group to which the provided parameter names apply."
    )
    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(final String groupName) {
        this.groupName = groupName;
    }

    @ApiModelProperty(
            value = "The sensitivity (SENSITIVE or NON_SENSITIVE) of the parameter group."
    )
    public String getSensitivity() {
        return sensitivity;
    }

    public void setSensitivity(final String sensitivity) {
        this.sensitivity = sensitivity;
    }

    /**
     * @return All fetched parameter names that should be applied.
     */
    @ApiModelProperty(
            value = "All fetched parameter names that should be applied."
    )
    public Set<String> getParameterNames() {
        return parameterNames;
    }

    public void setParameterNames(Set<String> parameterNames) {
        this.parameterNames = parameterNames;
    }

    @Override
    public int compareTo(final ProvidedParameterNameGroupEntity other) {
        if (other == null) {
            return -1;
        }

        final String groupName = getGroupName();
        final String otherGroupName = other.getGroupName();

        if (groupName == null) {
            return otherGroupName == null ? 0 : -1;
        }
        if (otherGroupName == null) {
            return 1;
        }
        return groupName.compareTo(otherGroupName);
    }
}
