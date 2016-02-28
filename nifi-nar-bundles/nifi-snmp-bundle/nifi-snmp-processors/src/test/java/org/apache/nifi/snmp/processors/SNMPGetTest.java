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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.agent.mo.DefaultMOFactory;
import org.snmp4j.agent.mo.MOAccessImpl;
import org.snmp4j.agent.mo.MOFactory;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;


/**
 * Test class used to check SNMP Get processor
 */
public class SNMPGetTest {

    /** agent for test with SNMP v2c */
    private static TestSnmpAgentV2c agentv2c = null;
    /** agent for test with SNMP v1 */
    private static TestSnmpAgentV1 agentv1 = null;

    /** OID to request */
    private static final OID sysDescr = new OID(".1.3.6.1.2.1.1.1.0");
    /** value we are supposed to retrieve */
    private static final String value = "MySystemDescr";

    /**
     * Method to set up different SNMP agents
     * @throws Exception Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        MOFactory factory = DefaultMOFactory.getInstance();

        agentv1 = new TestSnmpAgentV1("0.0.0.0/2002");
        agentv1.start();
        agentv1.unregisterManagedObject(agentv1.getSnmpv2MIB());
        agentv1.registerManagedObject(factory.createScalar(sysDescr,
                MOAccessImpl.ACCESS_READ_ONLY,
                new OctetString(value)));

        agentv2c = new TestSnmpAgentV2c("0.0.0.0/2001");
        agentv2c.start();
        agentv2c.unregisterManagedObject(agentv2c.getSnmpv2MIB());
        agentv2c.registerManagedObject(factory.createScalar(sysDescr,
                MOAccessImpl.ACCESS_READ_ONLY,
                new OctetString(value)));
    }

    /**
     * Method to close SNMP Agent once the tests are completed
     * @throws Exception Exception
     */
    @AfterClass
    public static void tearDown() throws Exception {
        agentv1.stop();
        agentv2c.stop();
    }

    /**
     * Method to test successful SNMP Get in case of v2c
     * @throws IOException IO Exception
     * @throws TimeoutException Timeout exception
     */
    @Test
    public void validateSuccessfulSnmpGetVersion2c() throws IOException, TimeoutException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/2001", SnmpConstants.version2c);
        try (SNMPGetter getter = new SNMPGetter(snmp, target, sysDescr)) {
            ResponseEvent response = getter.get();
            if(response.getResponse() == null) {
                fail();
            }
            assertEquals(value, response.getResponse().get(0).getVariable().toString());
        }
    }

    /**
     * Method to test successful SNMP Get in case of v1
     * @throws IOException IO Exception
     * @throws TimeoutException Timeout exception
     */
    @Test
    public void validateSuccessfulSnmpGetVersion1() throws IOException, TimeoutException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/2002", SnmpConstants.version1);
        try (SNMPGetter getter = new SNMPGetter(snmp, target, sysDescr)) {
            ResponseEvent response = getter.get();
            if(response.getResponse() == null) {
                fail();
            }
            assertEquals(value, response.getResponse().get(0).getVariable().toString());
        }
    }

}
