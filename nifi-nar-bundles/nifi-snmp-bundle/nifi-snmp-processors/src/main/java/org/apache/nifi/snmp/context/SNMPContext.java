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
package org.apache.nifi.snmp.context;

import org.apache.nifi.snmp.configuration.BasicConfiguration;
import org.apache.nifi.snmp.configuration.SecurityConfiguration;
import org.apache.nifi.snmp.exception.AgentSecurityConfigurationException;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.AbstractTarget;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.UserTarget;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.io.IOException;

public class SNMPContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(SNMPContext.class);

    private Snmp snmp;
    private AbstractTarget target;

    public static SNMPContext newInstance() {
        return new SNMPContext();
    }

    public void init(final BasicConfiguration basicConfiguration, final SecurityConfiguration securityConfiguration) {
        initSnmp(basicConfiguration);

        final String snmpVersion = securityConfiguration.getVersion();

        final int version = SNMPUtils.getSnmpVersion(snmpVersion);

        if (version == SnmpConstants.version3) {
            createUserTarget(basicConfiguration, securityConfiguration, snmp, version);
        } else {
            target = createCommunityTarget(basicConfiguration, securityConfiguration, version);
        }
    }

    public void close() {
        try {
            snmp.close();
        } catch (IOException e) {
            LOGGER.error("Could not close SNMP session.");
        }

    }

    private CommunityTarget createCommunityTarget(BasicConfiguration basicConfiguration, SecurityConfiguration securityConfiguration, int version) {
        CommunityTarget communityTarget = new CommunityTarget();
        setupTargetBasicProperties(communityTarget, basicConfiguration, version);
        String community = securityConfiguration.getCommunityString();
        if (community != null) {
            communityTarget.setCommunity(new OctetString(community));
        }
        return communityTarget;
    }

    private void createUserTarget(BasicConfiguration basicConfiguration, SecurityConfiguration securityConfiguration, Snmp snmp, int version) {
        final String username = securityConfiguration.getSecurityName();
        final String authProtocol = securityConfiguration.getAuthProtocol();
        final String authPassword = securityConfiguration.getAuthPassword();
        final String privacyProtocol = securityConfiguration.getPrivacyProtocol();
        final String privacyPassword = securityConfiguration.getPrivacyPassword();
        final OctetString authPasswordOctet = authPassword != null ? new OctetString(authPassword) : null;
        final OctetString privacyPasswordOctet = privacyPassword != null ? new OctetString(privacyPassword) : null;

        if (snmp.getUSM() == null) {
            throw new AgentSecurityConfigurationException("No security model has been configured in agent.");
        }

        // Add user information.
        snmp.getUSM().addUser(
                new OctetString(username),
                new UsmUser(new OctetString(username), SNMPUtils.getAuth(authProtocol), authPasswordOctet,
                        SNMPUtils.getPriv(privacyProtocol), privacyPasswordOctet));

        target = new UserTarget();
        setupTargetBasicProperties(target, basicConfiguration, version);
        int securityLevel = SecurityLevel.valueOf(securityConfiguration.getSecurityLevel()).getSnmpValue();
        target.setSecurityLevel(securityLevel);

        final String securityName = securityConfiguration.getSecurityName();
        if (securityName != null) {
            target.setSecurityName(new OctetString(securityName));
        }
    }

    private void initSnmp(final BasicConfiguration basicConfiguration) {
        int clientPort = basicConfiguration.getClientPort();
        try {
            snmp = new Snmp(new DefaultUdpTransportMapping(new UdpAddress("0.0.0.0/" + clientPort)));
            snmp.listen();
        } catch (IOException e) {
            LOGGER.error("Could not create transport mapping", e);
        }
    }

    private void setupTargetBasicProperties(AbstractTarget abstractTarget, BasicConfiguration basicConfiguration, int version) {
        final String host = basicConfiguration.getHost();
        final int port = basicConfiguration.getPort();
        final int retries = basicConfiguration.getRetries();
        final int timeout = basicConfiguration.getTimeout();

        abstractTarget.setVersion(version);
        abstractTarget.setAddress(new UdpAddress(host + "/" + port));
        abstractTarget.setRetries(retries);
        abstractTarget.setTimeout(timeout);
    }

    public Snmp getSnmp() {
        return snmp;
    }

    public AbstractTarget getTarget() {
        return target;
    }
}
