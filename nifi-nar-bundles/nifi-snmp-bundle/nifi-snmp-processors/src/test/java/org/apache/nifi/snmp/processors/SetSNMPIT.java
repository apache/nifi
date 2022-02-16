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

import org.apache.nifi.snmp.helper.testrunners.SNMPTestRunnerFactory;
import org.apache.nifi.snmp.helper.testrunners.SNMPV1TestRunnerFactory;
import org.apache.nifi.snmp.helper.testrunners.SNMPV2cTestRunnerFactory;
import org.apache.nifi.snmp.helper.testrunners.SNMPV3TestRunnerFactory;
import org.apache.nifi.snmp.testagents.TestAgent;
import org.apache.nifi.snmp.testagents.TestSNMPV1Agent;
import org.apache.nifi.snmp.testagents.TestSNMPV2cAgent;
import org.apache.nifi.snmp.testagents.TestSNMPV3Agent;
import org.apache.nifi.snmp.utils.SNMPUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.snmp4j.agent.mo.DefaultMOFactory;
import org.snmp4j.agent.mo.MOAccessImpl;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class SetSNMPIT {

    private static final String LOCALHOST = "127.0.0.1";
    private static final String TEST_OID = "1.3.6.1.4.1.32437.1.5.1.4.2.0";
    private static final String TEST_OID_VALUE = "TestOID";

    private static final SNMPTestRunnerFactory v1TestRunnerFactory = new SNMPV1TestRunnerFactory();
    private static final SNMPTestRunnerFactory v2cTestRunnerFactory = new SNMPV2cTestRunnerFactory();
    private static final SNMPTestRunnerFactory v3TestRunnerFactory = new SNMPV3TestRunnerFactory();

    private static final TestAgent v1TestAgent = new TestSNMPV1Agent(LOCALHOST);
    private static final TestAgent v2cTestAgent = new TestSNMPV2cAgent(LOCALHOST);
    private static final TestAgent v3TestAgent = new TestSNMPV3Agent(LOCALHOST);

    static {
        registerManagedObjects(v1TestAgent);
        registerManagedObjects(v2cTestAgent);
        registerManagedObjects(v3TestAgent);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {v1TestAgent, v1TestRunnerFactory},
                {v2cTestAgent, v2cTestRunnerFactory},
                {v3TestAgent, v3TestRunnerFactory}
        });
    }

    private final TestAgent testAgent;
    private final SNMPTestRunnerFactory testRunnerFactory;

    public SetSNMPIT(final TestAgent testAgent, final SNMPTestRunnerFactory testRunnerFactory) {
        this.testAgent = testAgent;
        this.testRunnerFactory = testRunnerFactory;
    }

    @Before
    public void setUp() throws IOException {
        testAgent.start();
    }

    @After
    public void tearDown() {
        testAgent.stop();
        testAgent.unregister();
    }


    @Test
    public void testSnmpSet() {
        final TestRunner runner = testRunnerFactory.createSnmpSetTestRunner(testAgent.getPort(), TEST_OID, TEST_OID_VALUE);
        runner.run();
        final MockFlowFile successFF = runner.getFlowFilesForRelationship(SetSNMP.REL_SUCCESS).get(0);

        assertNotNull(successFF);
        assertEquals(TEST_OID_VALUE, successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + TEST_OID + SNMPUtils.SNMP_PROP_DELIMITER + "4"));
    }

    private static void registerManagedObjects(final TestAgent agent) {
        agent.registerManagedObjects(
                DefaultMOFactory.getInstance().createScalar(new OID(TEST_OID), MOAccessImpl.ACCESS_READ_WRITE, new OctetString(TEST_OID_VALUE))
        );
    }
}
