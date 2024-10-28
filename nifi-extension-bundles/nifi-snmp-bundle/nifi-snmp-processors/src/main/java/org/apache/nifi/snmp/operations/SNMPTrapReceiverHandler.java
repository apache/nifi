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
package org.apache.nifi.snmp.operations;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.factory.core.SNMPManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snmp4j.Snmp;
import org.snmp4j.TransportMapping;
import org.snmp4j.mp.MPv3;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.TransportIpAddress;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class SNMPTrapReceiverHandler {

    private static final Logger logger = LoggerFactory.getLogger(SNMPTrapReceiverHandler.class);

    private final SNMPConfiguration configuration;
    private final List<UsmUser> usmUsers;
    private Snmp snmpManager;
    private boolean isStarted;

    public SNMPTrapReceiverHandler(final SNMPConfiguration configuration, final List<UsmUser> usmUsers) {
        this.configuration = configuration;
        this.usmUsers = usmUsers;
        snmpManager = new SNMPManagerFactory().createSnmpManagerInstance(configuration);
    }

    public int getListeningPort() {
        final Collection<TransportMapping<? extends Address>> transportMappings = snmpManager.getMessageDispatcher().getTransportMappings();
        if (transportMappings == null || transportMappings.isEmpty()) {
            return 0;
        }
        final Address address = transportMappings.iterator().next().getListenAddress();
        if (address instanceof TransportIpAddress) {
            return ((org.snmp4j.smi.TransportIpAddress) address).getPort();
        }

        return 0;
    }

    public void createTrapReceiver(final ProcessSessionFactory processSessionFactory, final ComponentLog logger) {
        addUsmUsers();
        SNMPTrapReceiver trapReceiver = new SNMPTrapReceiver(processSessionFactory, logger);
        snmpManager.addCommandResponder(trapReceiver);
        isStarted = true;
    }

    public boolean isStarted() {
        return isStarted;
    }

    public void close() {
        try {
            if (snmpManager.getUSM() != null) {
                snmpManager.getUSM().removeAllUsers();
                SecurityModels.getInstance().removeSecurityModel(new Integer32(snmpManager.getUSM().getID()));
            }
            snmpManager.close();
            isStarted = false;
        } catch (IOException e) {
            final String errorMessage = "Could not close SNMP manager.";
            logger.error(errorMessage, e);
            throw new ProcessException(errorMessage);
        }
    }

    private void addUsmUsers() {
        if (configuration.getVersion() == SnmpConstants.version3) {
            USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(MPv3.createLocalEngineID()), 0);
            SecurityModels.getInstance().addSecurityModel(usm);
            usmUsers.forEach(user -> snmpManager.getUSM().addUser(user));
        }
    }

    // Visible for testing.
    void setSnmpManager(final Snmp snmpManager) {
        this.snmpManager = snmpManager;
    }
}
