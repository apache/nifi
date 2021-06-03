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
package org.apache.nifi.snmp.testagents;

import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.snmp4j.TransportMapping;
import org.snmp4j.agent.BaseAgent;
import org.snmp4j.agent.CommandProcessor;
import org.snmp4j.agent.DuplicateRegistrationException;
import org.snmp4j.agent.MOGroup;
import org.snmp4j.agent.ManagedObject;
import org.snmp4j.agent.mo.snmp.RowStatus;
import org.snmp4j.agent.mo.snmp.SnmpCommunityMIB;
import org.snmp4j.agent.mo.snmp.SnmpNotificationMIB;
import org.snmp4j.agent.mo.snmp.SnmpTargetMIB;
import org.snmp4j.agent.mo.snmp.StorageType;
import org.snmp4j.security.USM;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.Variable;
import org.snmp4j.transport.TransportMappings;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public abstract class TestAgent extends BaseAgent {

    protected static final String BOOT_COUNTER_NAME_TEMPLATE = "target/bootCounter%s_%s.agent";
    protected static final String CONFIG_NAME_TEMPLATE = "target/conf%s_%s.agent";
    protected final String address;
    protected final int port;

    public TestAgent(final File bootCounterFile, final File configFile, final CommandProcessor commandProcessor, final String host) {
        super(bootCounterFile, configFile, commandProcessor);
        port = NetworkUtils.getAvailableUdpPort();
        this.address = String.format("udp:%s/%d", host, port);
    }

    @Override
    protected void initTransportMappings() {
        transportMappings = new TransportMapping[1];
        final Address transportAddress = GenericAddress.parse(address);
        final TransportMapping<? extends Address> transportMapping = TransportMappings.getInstance().createTransportMapping(transportAddress);
        transportMappings[0] = transportMapping;
    }

    public void start() throws IOException {
        init();
        addShutdownHook();
        getServer().addContext(new OctetString("public"));
        finishInit();
        run();
        sendColdStartNotification();
    }

    @Override
    protected void registerManagedObjects() {
    }

    public void registerManagedObject(ManagedObject mo) {
        try {
            server.register(mo, null);
        } catch (DuplicateRegistrationException ex) {
            throw new RuntimeException(ex);
        }
    }

    public void registerManagedObjects(final ManagedObject... managedObjects) {
        Arrays.stream(managedObjects).forEach(this::registerManagedObject);
    }

    @Override
    protected void unregisterManagedObjects() {
    }

    protected void unregisterManagedObject(final MOGroup moGroup) {
        moGroup.unregisterMOs(server, getContext(moGroup));
    }

    @Override
    protected void addUsmUser(final USM usm) {
    }

    @Override
    protected void addNotificationTargets(final SnmpTargetMIB targetMIB, final SnmpNotificationMIB notificationMIB) {
    }

    @Override
    protected void addCommunities(final SnmpCommunityMIB communityMIB) {
        final Variable[] com2sec = new Variable[]{
                new OctetString("public"), // community name
                new OctetString("cpublic"), // security name
                this.getAgent().getContextEngineID(), // local engine ID
                new OctetString("public"), // default context name
                new OctetString(), // transport tag
                new Integer32(StorageType.nonVolatile), // storage type
                new Integer32(RowStatus.active) // row status
        };
        final SnmpCommunityMIB.SnmpCommunityEntryRow row = communityMIB.getSnmpCommunityEntry().createRow(new OctetString("public2public")
                .toSubIndex(true), com2sec);
        communityMIB.getSnmpCommunityEntry().addRow(row);
    }

    public int getPort() {
        return port;
    }
}
