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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.snmp4j.AbstractTarget;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.agent.mo.DefaultMOFactory;
import org.snmp4j.agent.mo.MOAccessImpl;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

/**
 * Class to test SNMP Get processor
 */
public class SetSNMPTest {

    /** agent for version v1 */
    private static TestSnmpAgentV1 agentv1 = null;
    /** agent for version v2c */
    private static TestSnmpAgentV2c agentv2c = null;
    /** OID for system description */
    private static final OID sysDescr = new OID("1.3.6.1.2.1.1.1.0");
    /** value we set for system description at set-up */
    private static final String value = "MySystemDescr";
    /** OID for read only access */
    private static final OID readOnlyOID = new OID("1.3.6.1.2.1.1.3.0");
    /** value we set for read only at set-up */
    private static final int readOnlyValue = 1;
    /** OID for write only access */
    private static final OID writeOnlyOID = new OID("1.3.6.1.2.1.1.3.0");
    /** value we set for write only at set-up */
    private static final int writeOnlyValue = 1;

    /**
     * Method to set up different SNMP agents
     * @throws Exception Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        agentv2c = new TestSnmpAgentV2c("0.0.0.0");
        agentv2c.start();
        agentv2c.unregisterManagedObject(agentv2c.getSnmpv2MIB());
        agentv2c.registerManagedObject(
                DefaultMOFactory.getInstance().createScalar(sysDescr,
                        MOAccessImpl.ACCESS_READ_WRITE,
                        new OctetString(value)));
        agentv2c.registerManagedObject(
                DefaultMOFactory.getInstance().createScalar(readOnlyOID,
                        MOAccessImpl.ACCESS_READ_ONLY,
                        new Integer32(readOnlyValue)));

        agentv1 = new TestSnmpAgentV1("0.0.0.0");
        agentv1.start();
        agentv1.unregisterManagedObject(agentv1.getSnmpv2MIB());
        agentv1.registerManagedObject(
                DefaultMOFactory.getInstance().createScalar(sysDescr,
                        MOAccessImpl.ACCESS_READ_WRITE,
                        new OctetString(value)));
        agentv1.registerManagedObject(
                DefaultMOFactory.getInstance().createScalar(writeOnlyOID,
                        MOAccessImpl.ACCESS_WRITE_ONLY,
                        new Integer32(writeOnlyValue)));
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
     * Test the type success case during a SNMP set request
     * @throws Exception Exception
     */
    @Test
    public void successTypeSnmpSet() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + agentv1.getPort(), SnmpConstants.version1);

        SetSNMP pubProc = new LocalSetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        int syntax = new Integer32(writeOnlyValue).getSyntax();

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "bar");
        attributes.put("snmp$" + writeOnlyOID.toString() + "$" + syntax, String.valueOf(writeOnlyValue));
        runner.enqueue("".getBytes(), attributes);

        runner.setProperty(SetSNMP.HOST, "127.0.0.1");
        runner.setProperty(SetSNMP.PORT, String.valueOf(agentv1.getPort()));
        runner.setProperty(SetSNMP.SNMP_COMMUNITY, "public");
        runner.setProperty(SetSNMP.SNMP_VERSION, "SNMPv1");

        runner.run();
        Thread.sleep(200);
        final MockFlowFile successFF = runner.getFlowFilesForRelationship(SetSNMP.REL_SUCCESS).get(0);
        assertNotNull(successFF);

        pubProc.close();
    }

    /**
     * Test the type error case during a SNMP set request
     * @throws Exception Exception
     */
    @Test
    public void errorTypeSnmpSet() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + agentv1.getPort(), SnmpConstants.version1);

        SetSNMP pubProc = new LocalSetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "bar");
        attributes.put("snmp$" + writeOnlyOID.toString(), String.valueOf(writeOnlyValue));
        runner.enqueue("".getBytes(), attributes);

        runner.setProperty(SetSNMP.HOST, "127.0.0.1");
        runner.setProperty(SetSNMP.PORT, String.valueOf(agentv1.getPort()));
        runner.setProperty(SetSNMP.SNMP_COMMUNITY, "public");
        runner.setProperty(SetSNMP.SNMP_VERSION, "SNMPv1");

        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(SetSNMP.REL_SUCCESS).isEmpty());
        final MockFlowFile failFF = runner.getFlowFilesForRelationship(SetSNMP.REL_FAILURE).get(0);
        assertNotNull(failFF);
        assertEquals(String.valueOf(writeOnlyValue), failFF.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + writeOnlyOID.toString()));
        assertEquals("Bad value", failFF.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + "error"));
    }

    /**
     * Test the unauthorized case during a SNMP set request
     * @throws Exception Exception
     */
    @Test
    public void errorUnauthorizedSnmpSet() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + agentv2c.getPort(), SnmpConstants.version2c);

        SetSNMP pubProc = new LocalSetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "bar");
        attributes.put("snmp$" + readOnlyOID.toString(), String.valueOf(readOnlyValue));
        runner.enqueue("".getBytes(), attributes);

        runner.setProperty(SetSNMP.HOST, "127.0.0.1");
        runner.setProperty(SetSNMP.PORT, String.valueOf(agentv2c.getPort()));
        runner.setProperty(SetSNMP.SNMP_COMMUNITY, "public");
        runner.setProperty(SetSNMP.SNMP_VERSION, "SNMPv2c");

        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(SetSNMP.REL_SUCCESS).isEmpty());
        final MockFlowFile failFF = runner.getFlowFilesForRelationship(SetSNMP.REL_FAILURE).get(0);
        assertNotNull(failFF);
        assertEquals(String.valueOf(readOnlyValue), failFF.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + readOnlyOID.toString()));
        assertEquals("Not writable", failFF.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + "error"));
    }

    /**
     * Test the case with no OID in incoming FlowFile during a SNMP set request
     * @throws Exception Exception
     */
    @Test
    public void errorNoOIDSnmpSet() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + agentv2c.getPort(), SnmpConstants.version2c);

        SetSNMP pubProc = new LocalSetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "bar");
        runner.enqueue("".getBytes(), attributes);

        runner.setProperty(SetSNMP.HOST, "127.0.0.1");
        runner.setProperty(SetSNMP.PORT, String.valueOf(agentv2c.getPort()));
        runner.setProperty(SetSNMP.SNMP_COMMUNITY, "public");
        runner.setProperty(SetSNMP.SNMP_VERSION, "SNMPv2c");

        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(SetSNMP.REL_SUCCESS).isEmpty());
        assertTrue(runner.getFlowFilesForRelationship(SetSNMP.REL_FAILURE).size() == 1);
    }

    /**
     * Test the timeout case during a SNMP set request
     * @throws Exception Exception
     */
    @Test
    public void errorTimeoutSnmpSet() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + agentv2c.getPort(), SnmpConstants.version2c);

        SetSNMP pubProc = new LocalSetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "bar");
        attributes.put("snmp$" + "1.3.6.1.2.1.1.2.0", value);
        runner.enqueue("".getBytes(), attributes);

        runner.setProperty(SetSNMP.HOST, "127.0.0.1");
        runner.setProperty(SetSNMP.PORT, String.valueOf(SNMPTestUtil.availablePort()));
        runner.setProperty(SetSNMP.SNMP_COMMUNITY, "public");
        runner.setProperty(SetSNMP.SNMP_VERSION, "SNMPv2c");

        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(SetSNMP.REL_SUCCESS).isEmpty());
        assertTrue(runner.getFlowFilesForRelationship(SetSNMP.REL_FAILURE).size() == 1);
    }

    /**
     * Test the case noSuchObject during a SNMP set request
     * @throws Exception Exception
     */
    @Test
    public void errorNotExistingOIDSnmpSet() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + agentv2c.getPort(), SnmpConstants.version2c);

        SetSNMP pubProc = new LocalSetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "bar");
        attributes.put("snmp$" + "1.3.6.1.2.1.1.2.0", value);
        runner.enqueue("".getBytes(), attributes);

        runner.setProperty(SetSNMP.HOST, "127.0.0.1");
        runner.setProperty(SetSNMP.PORT, String.valueOf(agentv2c.getPort()));
        runner.setProperty(SetSNMP.SNMP_COMMUNITY, "public");
        runner.setProperty(SetSNMP.SNMP_VERSION, "SNMPv2c");

        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(SetSNMP.REL_SUCCESS).isEmpty());
        assertTrue(runner.getFlowFilesForRelationship(SetSNMP.REL_FAILURE).size() == 1);
        final MockFlowFile failFF = runner.getFlowFilesForRelationship(SetSNMP.REL_FAILURE).get(0);
        assertNotNull(failFF);
        assertEquals("Not writable", failFF.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + "error"));
    }

    /**
     * Test the case with not existing community during a SNMP set request
     * @throws Exception Exception
     */
    @Test
    public void errorCommunitySnmpSet() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("errorCommunity", "127.0.0.1/" + agentv2c.getPort(), SnmpConstants.version2c);

        SetSNMP pubProc = new LocalSetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "bar");
        attributes.put("snmp$" + sysDescr, value);
        runner.enqueue("".getBytes(), attributes);

        runner.setProperty(SetSNMP.HOST, "127.0.0.1");
        runner.setProperty(SetSNMP.PORT, String.valueOf(agentv2c.getPort()));
        runner.setProperty(SetSNMP.SNMP_COMMUNITY, "errorCommunity");
        runner.setProperty(SetSNMP.SNMP_VERSION, "SNMPv2c");

        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(SetSNMP.REL_SUCCESS).isEmpty());
        assertTrue(runner.getFlowFilesForRelationship(SetSNMP.REL_FAILURE).size() == 1);
    }

    /**
     * Local extension of SNMP Getter
     */
    private class LocalSetSnmp extends SetSNMP {
        /** SNMP */
        private Snmp snmp;
        /** Target to request */
        private AbstractTarget target;
        /**
         * Constructor
         * @param snmp SNMP
         * @param target Target
         */
        public LocalSetSnmp(Snmp snmp, AbstractTarget target) {
            this.snmp = snmp;
            this.target = target;
        }
    }

}
