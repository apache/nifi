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

import org.apache.nifi.snmp.testagents.TestSNMPV1Agent;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.snmp4j.agent.mo.DefaultMOFactory;
import org.snmp4j.agent.mo.MOAccessImpl;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class GetSNMPTest {

    private static TestSNMPV1Agent snmpV1Agent;
    private static final OID readOnlyOID1 = new OID("1.3.6.1.4.1.32437.1.5.1.4.2.0");
    private static final OID readOnlyOID2 = new OID("1.3.6.1.4.1.32437.1.5.1.4.3.0");
    private static final String OIDValue1 = "TestOID1";
    private static final String OIDValue2 = "TestOID2";
    private static final String GET = "GET";
    private static final String WALK = "WALK";

    @BeforeClass
    public static void setUp() throws IOException {
        snmpV1Agent = new TestSNMPV1Agent("127.0.0.1");
        snmpV1Agent.start();
        snmpV1Agent.registerManagedObjects(
                DefaultMOFactory.getInstance().createScalar(new OID(readOnlyOID1), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(OIDValue1)),
                DefaultMOFactory.getInstance().createScalar(new OID(readOnlyOID2), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(OIDValue2))
        );
    }

    @AfterClass
    public static void tearDown() {
        snmpV1Agent.stop();
    }

    @Test
    public void testSnmpV1Get() {
        final TestRunner runner = getTestRunner(readOnlyOID1.toString(), String.valueOf(snmpV1Agent.getPort()), GET);
        runner.run();
        final MockFlowFile successFF = runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).get(0);
        assertNotNull(successFF);
        assertEquals(OIDValue1, successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + readOnlyOID1.toString() + SNMPUtils.SNMP_PROP_DELIMITER + "4"));
    }

    @Test
    public void testSnmpV1Walk() {
        final TestRunner runner = getTestRunner("1.3.6.1.4.1.32437", String.valueOf(snmpV1Agent.getPort()), WALK);
        runner.run();
        final MockFlowFile successFF = runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).get(0);
        assertNotNull(successFF);
        assertEquals(OIDValue1, successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + readOnlyOID1.toString() + SNMPUtils.SNMP_PROP_DELIMITER + "4"));
        assertEquals(OIDValue2, successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + readOnlyOID2.toString() + SNMPUtils.SNMP_PROP_DELIMITER + "4"));
    }

    private TestRunner getTestRunner(final String oid, final String port, final String strategy) {
        final TestRunner runner = TestRunners.newTestRunner(GetSNMP.class);
        runner.setProperty(GetSNMP.OID, oid);
        runner.setProperty(GetSNMP.AGENT_HOST, "127.0.0.1");
        runner.setProperty(GetSNMP.AGENT_PORT, port);
        runner.setProperty(GetSNMP.SNMP_COMMUNITY, "public");
        runner.setProperty(GetSNMP.SNMP_VERSION, "SNMPv1");
        runner.setProperty(GetSNMP.SNMP_STRATEGY, strategy);
        runner.setProperty(GetSNMP.SNMP_SECURITY_LEVEL, "noAuthNoPriv");
        return runner;
    }
}
