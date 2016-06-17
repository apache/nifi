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
import static org.junit.Assert.fail;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.snmp4j.AbstractTarget;
import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.UserTarget;
import org.snmp4j.agent.mo.DefaultMOFactory;
import org.snmp4j.agent.mo.MOAccessImpl;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.MPv3;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.SecurityProtocols;
import org.snmp4j.security.USM;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

/**
 * Class to test SNMP Get processor
 */
public class GetSNMPTest {

    /** agent for version v1 */
    private static TestSnmpAgentV1 agentv1 = null;
    /** agent for version v2c */
    private static TestSnmpAgentV2c agentv2c = null;
    /** OID for system description */
    private static final OID sysDescr = new OID("1.3.6.1.2.1.1.1.0");
    /** value we set for system description at set-up */
    private static final String value = "MySystemDescr";
    /** OID for write only */
    private static final OID writeOnlyOID = new OID("1.3.6.1.2.1.1.3.0");
    /** value we set for write only at set-up */
    private static final int writeOnlyValue = 1;

    /**
     * Method to set up different SNMP agents
     * @throws Exception Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        agentv2c = new TestSnmpAgentV2c("0.0.0.0/2001");
        agentv2c.start();
        agentv2c.unregisterManagedObject(agentv2c.getSnmpv2MIB());
        agentv2c.registerManagedObject(
                DefaultMOFactory.getInstance().createScalar(sysDescr,
                        MOAccessImpl.ACCESS_READ_WRITE,
                        new OctetString(value)));
        agentv2c.registerManagedObject(
                DefaultMOFactory.getInstance().createScalar(writeOnlyOID,
                        MOAccessImpl.ACCESS_WRITE_ONLY,
                        new Integer32(writeOnlyValue)));

        agentv1 = new TestSnmpAgentV1("0.0.0.0/2002");
        agentv1.start();
        agentv1.unregisterManagedObject(agentv1.getSnmpv2MIB());
        agentv1.registerManagedObject(
                DefaultMOFactory.getInstance().createScalar(sysDescr,
                        MOAccessImpl.ACCESS_READ_WRITE,
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
     * Test to check FlowFile handling when performing a SNMP Get.
     * First we set a new value for the OID we want to request, then we
     * request this OID and we check that the returned value if the one
     * we set just before.
     * @throws Exception Exception
     */
    @Test
    public void validateSuccessfullSnmpSetGetv2c() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/2001", SnmpConstants.version2c);

        try (SNMPSetter setter = new SNMPSetter(snmp, target)) {
            PDU pdu = new PDU();
            pdu.add(new VariableBinding(new OID(sysDescr), new OctetString("test")));
            pdu.setType(PDU.SET);
            ResponseEvent response = setter.set(pdu);
            if(response.getResponse().getErrorStatus() != PDU.noError ) {
                fail();
            }
            Thread.sleep(200);

            GetSNMP pubProc = new LocalGetSnmp(snmp, target);
            TestRunner runner = TestRunners.newTestRunner(pubProc);

            runner.setProperty(GetSNMP.OID, sysDescr.toString());
            runner.setProperty(GetSNMP.HOST, "127.0.0.1");
            runner.setProperty(GetSNMP.PORT, "2001");
            runner.setProperty(GetSNMP.SNMP_COMMUNITY, "public");
            runner.setProperty(GetSNMP.SNMP_VERSION, "SNMPv2c");

            runner.run();
            Thread.sleep(200);
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).get(0);
            assertNotNull(successFF);
            assertEquals("test", successFF.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + sysDescr.toString() + SNMPUtils.SNMP_PROP_DELIMITER + "4"));

            pubProc.close();
        }
    }

    /**
     * Test to check FlowFile handling when performing a SNMP Get.
     * First we set a new value for the OID we want to request, then we
     * request this OID and we check that the returned value if the one
     * we set just before.
     * @throws Exception Exception
     */
    @Test
    public void validateSuccessfullSnmpSetGetv1() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/2002", SnmpConstants.version1);

        try (SNMPSetter setter = new SNMPSetter(snmp, target)) {
            PDU pdu = new PDU();
            pdu.add(new VariableBinding(new OID(sysDescr), new OctetString("test")));
            pdu.setType(PDU.SET);
            ResponseEvent response = setter.set(pdu);
            if(response.getResponse().getErrorStatus() != PDU.noError ) {
                fail();
            }
            Thread.sleep(200);

            GetSNMP pubProc = new LocalGetSnmp(snmp, target);
            TestRunner runner = TestRunners.newTestRunner(pubProc);
            runner.setProperty(GetSNMP.OID, sysDescr.toString());
            runner.setProperty(GetSNMP.HOST, "127.0.0.1");
            runner.setProperty(GetSNMP.PORT, "2002");
            runner.setProperty(GetSNMP.SNMP_COMMUNITY, "public");
            runner.setProperty(GetSNMP.SNMP_VERSION, "SNMPv1");

            runner.run();
            Thread.sleep(200);
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).get(0);
            assertNotNull(successFF);
            assertEquals("test", successFF.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + sysDescr.toString() + SNMPUtils.SNMP_PROP_DELIMITER + "4"));

            pubProc.close();
        }
    }

    /**
     * Test the unauthorized case during a SNMP get request
     * @throws Exception Exception
     */
    @Test
    public void errorUnauthorizedSnmpGet() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/2001", SnmpConstants.version2c);

        GetSNMP pubProc = new LocalGetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        runner.setProperty(GetSNMP.OID, writeOnlyOID.toString());
        runner.setProperty(GetSNMP.HOST, "127.0.0.1");
        runner.setProperty(GetSNMP.PORT, "2001");
        runner.setProperty(GetSNMP.SNMP_COMMUNITY, "public");
        runner.setProperty(GetSNMP.SNMP_VERSION, "SNMPv2c");

        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).isEmpty());
        final MockFlowFile failFF = runner.getFlowFilesForRelationship(GetSNMP.REL_FAILURE).get(0);
        assertNotNull(failFF);
        assertEquals("Null", failFF.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + writeOnlyOID.toString() + SNMPUtils.SNMP_PROP_DELIMITER + "5"));
        assertEquals("No access", failFF.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + "errorStatusText"));
    }

    /**
     * Test the timeout case during a SNMP get request
     * @throws Exception Exception
     */
    @Test
    public void errorTimeoutSnmpGet() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/2001", SnmpConstants.version2c);

        GetSNMP pubProc = new LocalGetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        runner.setProperty(GetSNMP.OID, sysDescr.toString());
        runner.setProperty(GetSNMP.HOST, "127.0.0.1");
        runner.setProperty(GetSNMP.PORT, "2010");
        runner.setProperty(GetSNMP.SNMP_COMMUNITY, "public");
        runner.setProperty(GetSNMP.SNMP_VERSION, "SNMPv2c");

        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).isEmpty());
        assertTrue(runner.getFlowFilesForRelationship(GetSNMP.REL_FAILURE).isEmpty());
    }

    /**
     * Test the case noSuchObject during a SNMP get request
     * @throws Exception Exception
     */
    @Test
    public void errorNotExistingOIDSnmpGet() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/2001", SnmpConstants.version2c);

        GetSNMP pubProc = new LocalGetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        runner.setProperty(GetSNMP.OID, "1.3.6.1.2.1.1.2.0");
        runner.setProperty(GetSNMP.HOST, "127.0.0.1");
        runner.setProperty(GetSNMP.PORT, "2001");
        runner.setProperty(GetSNMP.SNMP_COMMUNITY, "public");
        runner.setProperty(GetSNMP.SNMP_VERSION, "SNMPv2c");

        runner.run();
        Thread.sleep(200);
        final MockFlowFile successFF = runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).get(0);
        assertNotNull(successFF);
        assertEquals("noSuchObject", successFF.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + "1.3.6.1.2.1.1.2.0" + SNMPUtils.SNMP_PROP_DELIMITER + "128"));
    }

    /**
     * Test the case with not existing community during a SNMP get request
     * @throws Exception Exception
     */
    @Test
    public void errorCommunitySnmpGet() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("errorCommunity", "127.0.0.1/2001", SnmpConstants.version2c);

        GetSNMP pubProc = new LocalGetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        runner.setProperty(GetSNMP.OID, "1.3.6.1.2.1.1.2.0");
        runner.setProperty(GetSNMP.HOST, "127.0.0.1");
        runner.setProperty(GetSNMP.PORT, "2001");
        runner.setProperty(GetSNMP.SNMP_COMMUNITY, "errorCommunity");
        runner.setProperty(GetSNMP.SNMP_VERSION, "SNMPv2c");

        runner.run();
        Thread.sleep(200);
        assertTrue(runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).isEmpty());
        assertTrue(runner.getFlowFilesForRelationship(GetSNMP.REL_FAILURE).isEmpty());
    }

    /**
     * Method to test SNMP v3 cases
     * @throws Exception Exception
     */
    @Test
    public void validateSnmpVersion3() throws Exception {
        Thread thread= new Thread(new Runnable() {
            @Override
            public void run() {
                TestSnmpAgentV3.main(new String[]{"0.0.0.0/2003"});
            }
        });
        thread.start();

        DefaultUdpTransportMapping transportMapping = new DefaultUdpTransportMapping();
        Snmp snmp = new Snmp(transportMapping);
        USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(MPv3.createLocalEngineID()), 0);
        SecurityModels.getInstance().addSecurityModel(usm);
        transportMapping.listen();

        this.executeCase(snmp, "SHADES", "authPriv", "SHA", "SHADESPassword", "DES", "SHADESPassword");
        this.executeCase(snmp, "MD5DES", "authPriv", "MD5", "MD5DESAuthPassword", "DES", "MD5DESPrivPassword");
        this.executeCase(snmp, "SHAAES128", "authPriv", "SHA", "SHAAES128AuthPassword", "AES128", "SHAAES128PrivPassword");
    }

    /**
     * Method to test a specific configuration for SNMP V3
     * @param snmp SNMP
     * @param securityName Security name
     * @param securityLevel security level
     * @param authProt authentication protocol
     * @param authPwd authentication password
     * @param privProt private protocol
     * @param privPwd private password
     * @throws InterruptedException exception
     */
    private void executeCase(Snmp snmp, String securityName, String securityLevel, String authProt, String authPwd, String privProt, String privPwd) throws InterruptedException {
        UserTarget target = SNMPUtilsTest.prepareUser(snmp, "127.0.0.1/2003", SNMPUtils.getSecLevel(securityLevel),
                securityName, SNMPUtils.getAuth(authProt), SNMPUtils.getPriv(privProt), authPwd, privPwd);
        this.testTarget(snmp, target, securityName, securityLevel, authProt, authPwd, privProt, privPwd);
    }

    /**
     * Method to test a specific configuration for SNMP V3
     * @param snmp SNMP
     * @param target target
     * @param securityName Security name
     * @param securityLevel security level
     * @param authProt authentication protocol
     * @param authPwd authentication password
     * @param privProt private protocol
     * @param privPwd private password
     * @throws InterruptedException exception
     */
    private void testTarget(Snmp snmp, UserTarget target, String securityName, String securityLevel, String authProt, String authPwd, String privProt, String privPwd) throws InterruptedException {
        GetSNMP pubProc = new LocalGetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        runner.setProperty(GetSNMP.OID, sysDescr.toString());
        runner.setProperty(GetSNMP.HOST, "127.0.0.1");
        runner.setProperty(GetSNMP.PORT, "2003");
        runner.setProperty(GetSNMP.SNMP_VERSION, "SNMPv3");
        runner.setProperty(GetSNMP.SNMP_SECURITY_NAME, securityName);
        runner.setProperty(GetSNMP.SNMP_SECURITY_LEVEL, securityLevel);
        runner.setProperty(GetSNMP.SNMP_AUTH_PROTOCOL, authProt);
        if(authPwd != null) {
            runner.setProperty(GetSNMP.SNMP_AUTH_PASSWORD, authPwd);
        }
        runner.setProperty(GetSNMP.SNMP_PRIV_PROTOCOL, privProt);
        if(privPwd != null) {
            runner.setProperty(GetSNMP.SNMP_PRIV_PASSWORD, privPwd);
        }

        runner.run();
        Thread.sleep(200);

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).get(0);
        assertNotNull(successFF);
        assertTrue(successFF.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + sysDescr.toString() + SNMPUtils.SNMP_PROP_DELIMITER + "4").startsWith("SNMP4J-Agent"));
    }

    /**
     * Local extension of SNMP Getter
     */
    private class LocalGetSnmp extends GetSNMP {
        /** SNMP */
        private Snmp snmp;
        /** Target to request */
        private AbstractTarget target;
        /**
         * Constructor
         * @param snmp SNMP
         * @param target Target
         */
        public LocalGetSnmp(Snmp snmp, AbstractTarget target) {
            this.snmp = snmp;
            this.target = target;
        }
    }

}
