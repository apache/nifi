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
package org.apache.nifi.snmp.factory.core;

import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Target;
import org.snmp4j.smi.OctetString;

import java.util.Optional;

public class V1V2cSNMPFactory extends SNMPManagerFactory implements SNMPContext {

    @Override
    public Target<?> createTargetInstance(final SNMPConfiguration configuration) {
        final Target<?> communityTarget = new CommunityTarget<>();
        setupTargetBasicProperties(communityTarget, configuration);
        final String community = configuration.getCommunityString();

        Optional.ofNullable(community).map(OctetString::new).ifPresent(communityTarget::setSecurityName);

        return communityTarget;
    }
}
