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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.transport.DefaultUdpTransportMapping;
import org.snmp4j.util.TreeEvent;


/**
 * Test class used to check SNMP Walk part of the SNMP Get processor
 */
public class SNMPWalkTest {

    /** agent for test with SNMP v2c */
    private static TestSnmpAgentV2c agentv2c = null;
    /** agent for test with SNMP v1 */
    private static TestSnmpAgentV1 agentv1 = null;

    /** Root OID to perform a WALK */
    private static final OID root = new OID("1.3.6.1.2.1.1");

    /**
     * Method to set up different SNMP agents
     * @throws Exception Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        agentv1 = new TestSnmpAgentV1("0.0.0.0/2002");
        agentv1.start();
        agentv2c = new TestSnmpAgentV2c("0.0.0.0/2001");
        agentv2c.start();
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
     * Method to test successful SNMP Walk in case of v2c
     * @throws IOException IO Exception
     * @throws TimeoutException Timeout exception
     */
    @Test
    public void validateSuccessfulSnmpWalkVersion2c() throws IOException, TimeoutException {
        DefaultUdpTransportMapping transportMapping = new DefaultUdpTransportMapping();
        transportMapping.listen();
        Snmp snmp = new Snmp(transportMapping);

        CommunityTarget target = new CommunityTarget();
        target.setVersion(SnmpConstants.version2c);
        target.setCommunity(new OctetString("public"));
        target.setAddress(new UdpAddress("127.0.0.1/2001"));
        target.setRetries(0);
        target.setTimeout(500);

        try (SNMPGetter getter = new SNMPGetter(snmp, target, root)) {
            List<TreeEvent> response = getter.walk();
            assertEquals(response.size(), 1);
        }
    }

    /**
     * Method to test successful SNMP Walk in case of v1
     * @throws IOException IO Exception
     * @throws TimeoutException Timeout exception
     */
    @Test
    public void validateSuccessfullSnmpWalkVersion1() throws IOException, TimeoutException {
        DefaultUdpTransportMapping transportMapping = new DefaultUdpTransportMapping();
        transportMapping.listen();
        Snmp snmp = new Snmp(transportMapping);

        CommunityTarget target = new CommunityTarget();
        target.setVersion(SnmpConstants.version1);
        target.setCommunity(new OctetString("public"));
        target.setAddress(new UdpAddress("127.0.0.1/2002"));
        target.setRetries(0);
        target.setTimeout(500);

        try (SNMPGetter getter = new SNMPGetter(snmp, target, root)) {
            List<TreeEvent> response = getter.walk();
            assertEquals(response.size(), 9);
        }
    }

}
