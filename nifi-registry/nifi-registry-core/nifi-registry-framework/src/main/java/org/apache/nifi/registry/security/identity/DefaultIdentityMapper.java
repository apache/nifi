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
package org.apache.nifi.registry.security.identity;

import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.properties.util.IdentityMapping;
import org.apache.nifi.registry.properties.util.IdentityMappingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class DefaultIdentityMapper implements IdentityMapper {

    final List<IdentityMapping> userIdentityMappings;
    final List<IdentityMapping> groupMappings;

    @Autowired
    public DefaultIdentityMapper(final NiFiRegistryProperties properties) {
        userIdentityMappings = Collections.unmodifiableList(IdentityMappingUtil.getIdentityMappings(properties));
        groupMappings = Collections.unmodifiableList(IdentityMappingUtil.getGroupMappings(properties));
    }

    @Override
    public String mapUser(final String userIdentity) {
        return IdentityMappingUtil.mapIdentity(userIdentity, userIdentityMappings);
    }

    @Override
    public String mapGroup(final String groupName) {
        return IdentityMappingUtil.mapIdentity(groupName, groupMappings);
    }

}
