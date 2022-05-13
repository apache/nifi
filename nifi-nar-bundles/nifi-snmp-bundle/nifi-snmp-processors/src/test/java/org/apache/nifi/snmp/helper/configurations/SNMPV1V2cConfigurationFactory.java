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
package org.apache.nifi.snmp.helper.configurations;

import org.apache.nifi.snmp.configuration.SNMPConfiguration;

public class SNMPV1V2cConfigurationFactory implements SNMPConfigurationFactory {

    private final int snmpVersion;

    public SNMPV1V2cConfigurationFactory(int snmpVersion) {
        this.snmpVersion = snmpVersion;
    }

    @Override
    public SNMPConfiguration createSnmpGetSetConfiguration(final int agentPort) {
        return SNMPConfiguration.builder()
                .setTargetHost(LOCALHOST)
                .setTargetPort(String.valueOf(agentPort))
                .setCommunityString(COMMUNITY_STRING)
                .setVersion(snmpVersion)
                .build();
    }

    @Override
    public SNMPConfiguration createSnmpGetSetConfigWithCustomHost(final String host, final int agentPort) {
        return SNMPConfiguration.builder()
                .setTargetHost(host)
                .setTargetPort(String.valueOf(agentPort))
                .setCommunityString(COMMUNITY_STRING)
                .setVersion(snmpVersion)
                .build();
    }

    @Override
    public SNMPConfiguration createSnmpListenTrapConfig(final int managerPort) {
        return SNMPConfiguration.builder()
                .setManagerPort(managerPort)
                .setCommunityString(COMMUNITY_STRING)
                .setVersion(snmpVersion)
                .build();
    }
}
