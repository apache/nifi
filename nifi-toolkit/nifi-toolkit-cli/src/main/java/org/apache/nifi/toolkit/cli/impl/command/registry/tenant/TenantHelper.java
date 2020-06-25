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
package org.apache.nifi.toolkit.cli.impl.command.registry.tenant;

import org.apache.nifi.registry.authorization.Tenant;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class TenantHelper {
    private TenantHelper() {}

    public static <T extends Tenant> Set<Tenant> selectExistingTenants(final String names, final String ids, List<T> allTenants) {
        String separator = ",";

        Set<String> nameSet = new HashSet<>(Arrays.asList(Optional.ofNullable(names).orElse("").split(separator)));
        Set<String> idSet = new HashSet<>(Arrays.asList(Optional.ofNullable(ids).orElse("").split(separator)));

        Set<Tenant> existingTentants = allTenants.stream()
            .filter(tenant -> nameSet.contains(tenant.getIdentity()) || idSet.contains(tenant.getIdentifier()))
            .collect(Collectors.toSet());

        return existingTentants;
    }
}
