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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.parameter.ParameterSensitivity;
import org.apache.nifi.web.api.entity.ParameterGroupConfigurationEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class ParameterProviderMerger {

    public static void merge(final ParameterProviderEntity target, final ParameterProviderEntity otherEntity) {
        final Collection<ParameterGroupConfigurationEntity> targetParameterGroupConfigurations = target.getComponent().getParameterGroupConfigurations();
        if (targetParameterGroupConfigurations != null) {
            if (otherEntity.getComponent().getParameterGroupConfigurations() != null) {
                final Iterator<ParameterGroupConfigurationEntity> otherGroupIterator = otherEntity.getComponent().getParameterGroupConfigurations().iterator();
                for (final ParameterGroupConfigurationEntity parameterGroupConfiguration : targetParameterGroupConfigurations) {
                    if (!otherGroupIterator.hasNext()) {
                        continue;
                    }
                    ParameterGroupConfigurationEntity otherConfiguration = otherGroupIterator.next();
                    if (!StringUtils.equals(parameterGroupConfiguration.getGroupName(), otherConfiguration.getGroupName())) {
                        continue;
                    }
                    final Map<String, ParameterSensitivity> targetParameterSensitivities = parameterGroupConfiguration.getParameterSensitivities();
                    if (targetParameterSensitivities != null) {
                        if (otherConfiguration.getGroupName() != null) {
                            targetParameterSensitivities.keySet().retainAll(otherConfiguration.getParameterSensitivities().keySet());
                        }
                        parameterGroupConfiguration.setParameterSensitivities(new LinkedHashMap<>());
                        targetParameterSensitivities.keySet().stream()
                                .sorted()
                                .forEach(paramName -> parameterGroupConfiguration.getParameterSensitivities()
                                        .put(paramName, targetParameterSensitivities.get(paramName)));
                    }
                }
            }
        }
    }
}
