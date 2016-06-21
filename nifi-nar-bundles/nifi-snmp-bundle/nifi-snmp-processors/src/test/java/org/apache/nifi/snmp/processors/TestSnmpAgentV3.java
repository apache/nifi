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

import org.apache.log4j.BasicConfigurator;
import org.snmp4j.TransportMapping;
import org.snmp4j.agent.BaseAgent;
import org.snmp4j.agent.CommandProcessor;
import org.snmp4j.agent.io.ImportModes;
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
import org.snmp4j.security.AuthMD5;
import org.snmp4j.security.AuthSHA;
import org.snmp4j.security.PrivAES128;
import org.snmp4j.security.PrivDES;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.SecurityModel;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.Address;
import org.snmp4j.smi.GenericAddress;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.Variable;
import org.snmp4j.transport.TransportMappings;
import org.snmp4j.util.ThreadPool;

/**
 * The <code>TestAgent</code> is a sample SNMP agent implementation of all
 * features (MIB implementations) provided by the SNMP4J-Agent framework.
 * The <code>TestAgent</code> extends the <code>BaseAgent</code> which provides
 * a framework for custom agent implementations through hook methods. Those
 * abstract hook methods need to be implemented by extending the
 * <code>BaseAgent</code>.
 * <p>
 * This IF-MIB implementation part of this test agent, is instrumentation as
 * a simulation MIB. Thus, by changing the agentppSimMode
 * (1.3.6.1.4.1.4976.2.1.1.0) from 'oper(1)' to 'config(2)' any object of the
 * IF-MIB is writable and even creatable (columnar objects) via SNMP. Check it
 * out!
 *
 * @author Frank Fock
 * @version 1.0
 */
public class TestSnmpAgentV3 extends BaseAgent {

    // initialize Log4J logging
    static {
        LogFactory.setLogFactory(new Log4jLogFactory());
    }

    /** address */
    protected String address;

    /**
     * Creates the test agent with a file to read and store the boot counter and
     * a file to read and store its configuration.
     *
     * @param bootCounterFile
     *    a file containing the boot counter in serialized form (as expected by
     *    BaseAgent).
     * @param configFile
     *    a configuration file with serialized management information.
     * @throws IOException
     *    if the boot counter or config file cannot be read properly.
     */
    public TestSnmpAgentV3(File bootCounterFile, File configFile) throws IOException {
        super(bootCounterFile, configFile, new CommandProcessor(OctetString.fromHexString("00:00:00:00:00:00:02", ':')));
        this.agent.setWorkerPool(ThreadPool.create("RequestPool", 4));
    }

    /**
     * @see org.snmp4j.agent.BaseAgent#registerManagedObjects()
     */
    @Override
    protected void registerManagedObjects() {
        /** nothing to do */
    }

    /**
     * @see org.snmp4j.agent.BaseAgent#addNotificationTargets(org.snmp4j.agent.mo.snmp.SnmpTargetMIB, org.snmp4j.agent.mo.snmp.SnmpNotificationMIB)
     */
    @Override
    protected void addNotificationTargets(SnmpTargetMIB targetMIB, SnmpNotificationMIB notificationMIB) {
        /** nothing to do */
    }

    /**
     * @see org.snmp4j.agent.BaseAgent#addViews(org.snmp4j.agent.mo.snmp.VacmMIB)
     */
    @Override
    protected void addViews(VacmMIB vacm) {
        vacm.addGroup(SecurityModel.SECURITY_MODEL_USM,
                new OctetString("SHADES"),
                new OctetString("v3group"),
                StorageType.nonVolatile);
        vacm.addGroup(SecurityModel.SECURITY_MODEL_USM,
                new OctetString("MD5DES"),
                new OctetString("v3group"),
                StorageType.nonVolatile);
        vacm.addGroup(SecurityModel.SECURITY_MODEL_USM,
                new OctetString("SHAAES128"),
                new OctetString("v3group"),
                StorageType.nonVolatile);
        vacm.addAccess(new OctetString("v3group"), new OctetString(),
                SecurityModel.SECURITY_MODEL_USM,
                SecurityLevel.AUTH_PRIV,
                MutableVACM.VACM_MATCH_EXACT,
                new OctetString("fullReadView"),
                new OctetString("fullWriteView"),
                new OctetString("fullNotifyView"),
                StorageType.nonVolatile);
        vacm.addViewTreeFamily(new OctetString("fullReadView"), new OID("1.3"),
                new OctetString(), VacmMIB.vacmViewIncluded,
                StorageType.nonVolatile);
        vacm.addViewTreeFamily(new OctetString("fullWriteView"), new OID("1.3"),
                new OctetString(), VacmMIB.vacmViewIncluded,
                StorageType.nonVolatile);
        vacm.addViewTreeFamily(new OctetString("fullNotifyView"), new OID("1.3"),
                new OctetString(), VacmMIB.vacmViewIncluded,
                StorageType.nonVolatile);
    }

    /**
     * @see org.snmp4j.agent.BaseAgent#addUsmUser(org.snmp4j.security.USM)
     */
    @Override
    protected void addUsmUser(USM usm) {
        UsmUser user = new UsmUser(new OctetString("SHA"),
                AuthSHA.ID,
                new OctetString("SHAAuthPassword"),
                null,
                null);
        usm.addUser(user.getSecurityName(), usm.getLocalEngineID(), user);
        user = new UsmUser(new OctetString("SHADES"),
                AuthSHA.ID,
                new OctetString("SHADESAuthPassword"),
                PrivDES.ID,
                new OctetString("SHADESPrivPassword"));
        usm.addUser(user.getSecurityName(), usm.getLocalEngineID(), user);
        user = new UsmUser(new OctetString("MD5DES"),
                AuthMD5.ID,
                new OctetString("MD5DESAuthPassword"),
                PrivDES.ID,
                new OctetString("MD5DESPrivPassword"));
        usm.addUser(user.getSecurityName(), usm.getLocalEngineID(), user);
        user = new UsmUser(new OctetString("SHAAES128"),
                AuthSHA.ID,
                new OctetString("SHAAES128AuthPassword"),
                PrivAES128.ID,
                new OctetString("SHAAES128PrivPassword"));
        usm.addUser(user.getSecurityName(), usm.getLocalEngineID(), user);
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
     * Method to run agent
     * @param args arguments
     */
    public static void main(String[] args) {
        String address = args[0] + "/" + SNMPTestUtil.availablePort();;
        BasicConfigurator.configure();
        try {
            TestSnmpAgentV3 testAgent1 = new TestSnmpAgentV3(new File("target/SNMP4JTestAgentBC.cfg"),
                    new File("target/SNMP4JTestAgentConfig.cfg"));
            testAgent1.address = address;
            testAgent1.init();
            testAgent1.loadConfig(ImportModes.REPLACE_CREATE);
            testAgent1.addShutdownHook();
            testAgent1.getServer().addContext(new OctetString("public"));
            testAgent1.finishInit();
            testAgent1.run();
            testAgent1.sendColdStartNotification();
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * @see org.snmp4j.agent.BaseAgent#unregisterManagedObjects()
     */
    @Override
    protected void unregisterManagedObjects() {
        /** nothing to do */
    }

    /**
     * @see org.snmp4j.agent.BaseAgent#addCommunities(org.snmp4j.agent.mo.snmp.SnmpCommunityMIB)
     */
    @Override
    protected void addCommunities(SnmpCommunityMIB communityMIB) {
        Variable[] com2sec = new Variable[] {
                new OctetString("public"),              // community name
                new OctetString("cpublic"),             // security name
                this.getAgent().getContextEngineID(),   // local engine ID
                new OctetString("public"),              // default context name
                new OctetString(),                      // transport tag
                new Integer32(StorageType.nonVolatile), // storage type
                new Integer32(RowStatus.active)         // row status
        };
        MOTableRow row = communityMIB.getSnmpCommunityEntry().createRow(new OctetString("public2public").toSubIndex(true), com2sec);
        communityMIB.getSnmpCommunityEntry().addRow(row);
    }

    /**
     * @see org.snmp4j.agent.BaseAgent#registerSnmpMIBs()
     */
    @Override
    protected void registerSnmpMIBs() {
        super.registerSnmpMIBs();
    }
}
