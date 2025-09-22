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
package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.commons.lang3.Strings;
import org.apache.nifi.cluster.manager.PermissionsDtoMerger;
import org.apache.nifi.parameter.ParameterSensitivity;
import org.apache.nifi.web.api.entity.ParameterGroupConfigurationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class ParameterProviderMerger {

    public static void merge(final ParameterProviderEntity target, final ParameterProviderEntity otherEntity) {
        if (target == null || otherEntity == null) {
            return;
        }

        // Merge permissions first to ensure consistent visibility decisions
        if (target.getPermissions() != null && otherEntity.getPermissions() != null) {
            PermissionsDtoMerger.mergePermissions(target.getPermissions(), otherEntity.getPermissions());
        }

        // If either side has no readable component, clear the component on the target
        if (!Boolean.TRUE.equals(target.getPermissions() != null ? target.getPermissions().getCanRead() : Boolean.TRUE)
                || target.getComponent() == null || otherEntity.getComponent() == null) {
            target.setComponent(null);
            return;
        }

        final Collection<ParameterGroupConfigurationEntity> targetParameterGroupConfigurations = target.getComponent().getParameterGroupConfigurations();
        final Collection<ParameterGroupConfigurationEntity> otherParameterGroupConfigurations = otherEntity.getComponent().getParameterGroupConfigurations();

        if (targetParameterGroupConfigurations == null || otherParameterGroupConfigurations == null) {
            return;
        }

        final Iterator<ParameterGroupConfigurationEntity> otherGroupIterator = otherParameterGroupConfigurations.iterator();
        for (final ParameterGroupConfigurationEntity parameterGroupConfiguration : targetParameterGroupConfigurations) {
            if (!otherGroupIterator.hasNext()) {
                continue;
            }

            final ParameterGroupConfigurationEntity otherConfiguration = otherGroupIterator.next();
            if (!Strings.CS.equals(parameterGroupConfiguration.getGroupName(), otherConfiguration.getGroupName())) {
                continue;
            }

            final Map<String, ParameterSensitivity> targetParameterSensitivities = parameterGroupConfiguration.getParameterSensitivities();
            final Map<String, ParameterSensitivity> otherParameterSensitivities = otherConfiguration.getParameterSensitivities();

            if (targetParameterSensitivities == null || otherParameterSensitivities == null) {
                continue;
            }

            // Keep only the parameters present on both, then sort keys deterministically
            targetParameterSensitivities.keySet().retainAll(otherParameterSensitivities.keySet());

            parameterGroupConfiguration.setParameterSensitivities(new LinkedHashMap<>());
            targetParameterSensitivities.keySet().stream()
                .sorted()
                .forEach(paramName -> parameterGroupConfiguration.getParameterSensitivities()
                    .put(paramName, targetParameterSensitivities.get(paramName)));
        }
    }
}
