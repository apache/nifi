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
import org.apache.nifi.snmp.exception.CreateSNMPClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.UserTarget;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.io.IOException;
import java.util.Optional;

public abstract class AbstractSNMPFactory {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSNMPFactory.class);

    protected AbstractSNMPFactory() {
        // hide implicit constructor
    }

    protected static Snmp createSnmpClient() {
        final Snmp snmp;
        try {
            snmp = new Snmp(new DefaultUdpTransportMapping());
            snmp.listen();
            return snmp;
        } catch (IOException e) {
            final String errorMessage = "Creating SNMP client failed.";
            logger.error(errorMessage, e);
            throw new CreateSNMPClientException(errorMessage);
        }
    }

    protected static Target createUserTarget(final SNMPConfiguration configuration) {
        final UserTarget userTarget = new UserTarget();
        setupTargetBasicProperties(userTarget, configuration);

        final int securityLevel = SecurityLevel.valueOf(configuration.getSecurityLevel()).getSnmpValue();
        userTarget.setSecurityLevel(securityLevel);

        final String securityName = configuration.getSecurityName();
        Optional.ofNullable(securityName).map(OctetString::new).ifPresent(userTarget::setSecurityName);

        return userTarget;
    }

    protected static Target createCommunityTarget(final SNMPConfiguration configuration) {
        final Target communityTarget = new CommunityTarget();
        setupTargetBasicProperties(communityTarget, configuration);
        final String community = configuration.getCommunityString();

        Optional.ofNullable(community).map(OctetString::new).ifPresent(communityTarget::setSecurityName);

        return communityTarget;
    }

    private static void setupTargetBasicProperties(final Target target, final SNMPConfiguration configuration) {
        final int snmpVersion = configuration.getVersion();
        final String host = configuration.getAgentHost();
        final String port = configuration.getAgentPort();
        final int retries = configuration.getRetries();
        final int timeout = configuration.getTimeout();

        target.setVersion(snmpVersion);
        target.setAddress(new UdpAddress(host + "/" + port));
        target.setRetries(retries);
        target.setTimeout(timeout);
    }
}
