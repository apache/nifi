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
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.UserTarget;
import org.snmp4j.mp.MPv3;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import java.util.Optional;

public class V3SNMPFactory extends SNMPManagerFactory implements SNMPContext {

    @Override
    public Snmp createSnmpManagerInstance(final SNMPConfiguration configuration) {
        final Snmp snmpManager = super.createSnmpManagerInstance(configuration);

        // Create USM.
        final OctetString localEngineId = new OctetString(MPv3.createLocalEngineID());
        final USM usm = new USM(SecurityProtocols.getInstance(), localEngineId, 0);
        SecurityModels.getInstance().addSecurityModel(usm);

        Optional.ofNullable(configuration.getSecurityName())
                .map(OctetString::new)
                .ifPresent(securityName -> {
                    OID authProtocol = Optional.ofNullable(configuration.getAuthProtocol())
                            .map(SNMPUtils::getAuth).orElse(null);
                    OctetString authPassphrase = Optional.ofNullable(configuration.getAuthPassphrase())
                            .map(OctetString::new).orElse(null);
                    OID privacyProtocol = Optional.ofNullable(configuration.getPrivacyProtocol())
                            .map(SNMPUtils::getPriv).orElse(null);
                    OctetString privacyPassphrase = Optional.ofNullable(configuration.getPrivacyPassphrase())
                            .map(OctetString::new).orElse(null);
                    snmpManager.getUSM().addUser(securityName, new UsmUser(securityName, authProtocol, authPassphrase,
                            privacyProtocol, privacyPassphrase));
                });

        return snmpManager;
    }

    @Override
    public Target<?> createTargetInstance(final SNMPConfiguration configuration) {
        final UserTarget<?> userTarget = new UserTarget<>();
        setupTargetBasicProperties(userTarget, configuration);

        final int securityLevel = SecurityLevel.valueOf(configuration.getSecurityLevel()).getSnmpValue();
        userTarget.setSecurityLevel(securityLevel);

        final String securityName = configuration.getSecurityName();
        Optional.ofNullable(securityName).map(OctetString::new).ifPresent(userTarget::setSecurityName);

        return userTarget;
    }
}
