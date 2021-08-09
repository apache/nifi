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
package org.apache.nifi.snmp.factory;

import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.mp.MPv3;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import java.util.Optional;

public class V3SNMPFactory extends AbstractSNMPFactory implements SNMPFactory {

    @Override
    public boolean supports(final int version) {
        return SnmpConstants.version3 == version;
    }

    @Override
    public Snmp createSnmpManagerInstance(final SNMPConfiguration configuration) {
        final Snmp snmp = createSnmpClient();

        // If there's a USM instance associated with the MPv3 bound to this Snmp instance (like an agent running
        // on the same host) it is not null.
        if (snmp.getUSM() == null) {
            final OctetString localEngineId = new OctetString(MPv3.createLocalEngineID());
            final USM usm = new USM(SecurityProtocols.getInstance(), localEngineId, 0);
            SecurityModels.getInstance().addSecurityModel(usm);
        }

        final String username = configuration.getSecurityName();
        final OID authProtocol = Optional.ofNullable(configuration.getAuthProtocol())
                .map(SNMPUtils::getAuth).orElse(null);
        final OID privacyProtocol = Optional.ofNullable(configuration.getPrivacyProtocol())
                .map(SNMPUtils::getPriv).orElse(null);
        final String authPassword = configuration.getAuthPassphrase();
        final String privacyPassword = configuration.getPrivacyPassphrase();
        final OctetString authPasswordOctet = authPassword != null ? new OctetString(authPassword) : null;
        final OctetString privacyPasswordOctet = privacyPassword != null ? new OctetString(privacyPassword) : null;

        // Add user information.
        snmp.getUSM().addUser(
                new OctetString(username),
                new UsmUser(new OctetString(username), authProtocol, authPasswordOctet,
                        privacyProtocol, privacyPasswordOctet));

        return snmp;
    }

    @Override
    public Target createTargetInstance(final SNMPConfiguration configuration) {
        return createUserTarget(configuration);
    }
}
