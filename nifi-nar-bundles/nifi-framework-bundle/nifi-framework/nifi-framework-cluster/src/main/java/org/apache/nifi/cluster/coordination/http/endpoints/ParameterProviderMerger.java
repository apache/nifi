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
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ProvidedParameterNameGroupEntity;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class ParameterProviderMerger {

    public static void merge(final ParameterProviderEntity target, final ParameterProviderEntity otherEntity) {
        final Collection<ProvidedParameterNameGroupEntity> targetParameterNameGroups = target.getComponent().getFetchedParameterNameGroups();
        if (targetParameterNameGroups != null) {
            if (otherEntity.getComponent().getFetchedParameterNameGroups() != null) {
                final Iterator<ProvidedParameterNameGroupEntity> otherGroupIterator = otherEntity.getComponent().getFetchedParameterNameGroups().iterator();
                for (final ProvidedParameterNameGroupEntity targetParameterNameGroup : targetParameterNameGroups) {
                    if (!otherGroupIterator.hasNext()) {
                        continue;
                    }
                    ProvidedParameterNameGroupEntity otherGroup = otherGroupIterator.next();
                    if (!StringUtils.equals(targetParameterNameGroup.getGroupName(), otherGroup.getGroupName())) {
                        continue;
                    }
                    final Set<String> targetParameterNames = targetParameterNameGroup.getParameterNames();
                    if (targetParameterNames != null) {
                        if (otherGroup.getGroupName() != null) {
                            targetParameterNames.retainAll(otherGroup.getParameterNames());
                        }
                        targetParameterNameGroup.setParameterNames(new LinkedHashSet<>(targetParameterNames.stream().sorted().collect(Collectors.toList())));
                    }
                }
            }
        }
    }
}
