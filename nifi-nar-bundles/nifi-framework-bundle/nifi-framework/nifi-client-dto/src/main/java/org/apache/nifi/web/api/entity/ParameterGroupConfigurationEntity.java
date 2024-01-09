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

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.nifi.parameter.ParameterSensitivity;

import jakarta.xml.bind.annotation.XmlType;
import java.util.Map;

/**
 * Entity encapsulating the configuration for a single parameter group.
 */
@XmlType(name = "parameterGroupConfiguration")
public class ParameterGroupConfigurationEntity extends Entity implements Comparable<ParameterGroupConfigurationEntity> {

    private String groupName;
    private String parameterContextName;
    private Boolean isSynchronized;
    private Map<String, ParameterSensitivity> parameterSensitivities;

    @Schema(description = "The name of the external parameter group to which the provided parameter names apply."
    )
    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(final String groupName) {
        this.groupName = groupName;
    }

    @Schema(description = "The name of the ParameterContext that receives the parameters in this group"
    )
    public String getParameterContextName() {
        return parameterContextName;
    }

    public void setParameterContextName(final String parameterContextName) {
        this.parameterContextName = parameterContextName;
    }

    /**
     * @return All fetched parameter names that should be applied.
     */
    @Schema(description = "All fetched parameter names that should be applied."
    )
    public Map<String, ParameterSensitivity> getParameterSensitivities() {
        return parameterSensitivities;
    }

    public void setParameterSensitivities(Map<String, ParameterSensitivity> parameterSensitivities) {
        this.parameterSensitivities = parameterSensitivities;
    }

    @Schema(description = "True if this group should be synchronized to a ParameterContext, including creating one if it does not exist."
    )
    public Boolean isSynchronized() {
        return isSynchronized;
    }

    public void setSynchronized(Boolean aSynchronized) {
        isSynchronized = aSynchronized;
    }

    @Override
    public int compareTo(final ParameterGroupConfigurationEntity other) {
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
