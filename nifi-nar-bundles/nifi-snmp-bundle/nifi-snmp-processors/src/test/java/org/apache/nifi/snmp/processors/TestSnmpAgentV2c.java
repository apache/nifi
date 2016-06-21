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
package org.apache.nifi.snmp.processors;


import java.io.File;
import java.io.IOException;

import org.snmp4j.TransportMapping;
import org.snmp4j.agent.BaseAgent;
import org.snmp4j.agent.CommandProcessor;
import org.snmp4j.agent.DuplicateRegistrationException;
import org.snmp4j.agent.MOGroup;
import org.snmp4j.agent.ManagedObject;
import org.snmp4j.agent.mo.MOTableRow;
import org.snmp4j.agent.mo.snmp.RowStatus;
import org.snmp4j.agent.mo.snmp.SnmpCommunityMIB;
import org.snmp4j.agent.mo.snmp.SnmpNotificationMIB;
import org.snmp4j.agent.mo.snmp.SnmpTargetMIB;
import org.snmp4j.agent.mo.snmp.StorageType;
import org.snmp4j.agent.mo.snmp.VacmMIB;
import org.snmp4j.agent.security.MutableVACM;
import org.snmp4j.log.Log4jLogFactory;
import org.snmp4j.log.LogFactory;
import org.snmp4j.mp.MPv3;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.SecurityModel;
import org.snmp4j.security.USM;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.Variable;
import org.snmp4j.transport.TransportMappings;

/**
 * This Agent contains minimal functionality for running a version 2c SNMP agent.
 */
public class TestSnmpAgentV2c extends BaseAgent {

    // not needed but very useful of course
    static {
        LogFactory.setLogFactory(new Log4jLogFactory());
    }

    /** address */
    private String address;
    /** port */
    private int port;

    /** constructor
     * @param address address
     * @throws IOException IO Exception
     */
    public TestSnmpAgentV2c(String address) throws IOException {
        // These files have to be specified
        // Read snmp4j doc for more info
        super(new File("target/conf2.agent"), new File("target/bootCounter2.agent"), new CommandProcessor(new OctetString(MPv3.createLocalEngineID())));
        this.port = SNMPTestUtil.availablePort();
        this.address = address + "/" + port;
    }

    /**
     * We let clients of this agent register the MO they
     * need so this method does nothing
     */
    @Override
    protected void registerManagedObjects() {
    }

    /**
     * Clients can register the MO they need
     * @param mo managed object
     */
    public void registerManagedObject(ManagedObject mo) {
        try {
            this.server.register(mo, null);
        } catch (DuplicateRegistrationException ex) {
            throw new RuntimeException(ex);
        }
    }

    /** Method used to unregister objects
     * @param moGroup group to unregister
     */
    public void unregisterManagedObject(MOGroup moGroup) {
        moGroup.unregisterMOs(this.server, this.getContext(moGroup));
    }

    /**
     * @see org.snmp4j.agent.BaseAgent#addNotificationTargets(org.snmp4j.agent.mo.snmp.SnmpTargetMIB, org.snmp4j.agent.mo.snmp.SnmpNotificationMIB)
     */
    @Override
    protected void addNotificationTargets(SnmpTargetMIB targetMIB, SnmpNotificationMIB notificationMIB) {
        /** nothing to do */
    }

    /**
     * Minimal View based Access Control
     * http://www.faqs.org/rfcs/rfc2575.html
     */
    @Override
    protected void addViews(VacmMIB vacm) {

        vacm.addGroup(SecurityModel.SECURITY_MODEL_SNMPv2c,
                new OctetString("cpublic"),
                new OctetString("v1v2group"),
                StorageType.nonVolatile);

        vacm.addAccess(new OctetString("v1v2group"),
                new OctetString("public"),
                SecurityModel.SECURITY_MODEL_ANY,
                SecurityLevel.NOAUTH_NOPRIV,
                MutableVACM.VACM_MATCH_EXACT,
                new OctetString("fullReadView"),
                new OctetString("fullWriteView"),
                new OctetString("fullNotifyView"),
                StorageType.nonVolatile);

        vacm.addViewTreeFamily(new OctetString("fullReadView"),
                new OID("1.3"),
                new OctetString(),
                VacmMIB.vacmViewIncluded,
                StorageType.nonVolatile);
        vacm.addViewTreeFamily(new OctetString("fullWriteView"),
                new OID("1.3"),
                new OctetString(),
                VacmMIB.vacmViewIncluded,
                StorageType.nonVolatile);
        vacm.addViewTreeFamily(new OctetString("fullNotifyView"),
                new OID("1.3"),
                new OctetString(),
                VacmMIB.vacmViewIncluded,
                StorageType.nonVolatile);
    }

    /**
     * User based Security Model, only applicable to SNMP v.3
     */
    @Override
    protected void addUsmUser(USM usm) {
        /** nothing to do */
    }

    /**
     * @see org.snmp4j.agent.BaseAgent#initTransportMappings()
     */
    @Override
    protected void initTransportMappings() throws IOException {
        this.transportMappings = new TransportMapping[1];
        Address addr = GenericAddress.parse(this.address);
        TransportMapping tm = TransportMappings.getInstance().createTransportMapping(addr);
        this.transportMappings[0] = tm;
    }

    /**
     * Start method invokes some initialization methods needed to
     * start the agent
     * @throws IOException IO Exception
     */
    public void start() throws IOException {
        this.init();
        this.addShutdownHook();
        this.getServer().addContext(new OctetString("public"));
        this.finishInit();
        this.run();
        this.sendColdStartNotification();
    }

    /**
     * @see org.snmp4j.agent.BaseAgent#unregisterManagedObjects()
     */
    @Override
    protected void unregisterManagedObjects() {
        /** nothing to do */
    }

    /**
     * The table of community strings configured in the SNMP
     * engine's Local Configuration Datastore (LCD).
     *
     * We only configure one, "public".
     */
    @Override
    protected void addCommunities(SnmpCommunityMIB communityMIB) {
        Variable[] com2sec = new Variable[] {
                new OctetString("public"), // community name
                new OctetString("cpublic"), // security name
                this.getAgent().getContextEngineID(), // local engine ID
                new OctetString("public"), // default context name
                new OctetString(), // transport tag
                new Integer32(StorageType.nonVolatile), // storage type
                new Integer32(RowStatus.active) // row status
        };

        MOTableRow row = communityMIB.getSnmpCommunityEntry().createRow(new OctetString("public2public").toSubIndex(true), com2sec);

        communityMIB.getSnmpCommunityEntry().addRow(row);
    }

    public int getPort() {
        return port;
    }
}
