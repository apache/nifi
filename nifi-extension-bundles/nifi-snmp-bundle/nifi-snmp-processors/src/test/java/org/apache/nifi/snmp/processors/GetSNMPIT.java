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

import org.apache.commons.lang3.StringUtils;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.snmp4j.agent.mo.DefaultMOFactory;
import org.snmp4j.agent.mo.MOAccessImpl;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class GetSNMPIT {

    private static final String LOCALHOST = "127.0.0.1";
    private static final String READ_ONLY_OID_1 = "1.3.6.1.4.1.32437.1.5.1.4.2.0";
    private static final String READ_ONLY_OID_2 = "1.3.6.1.4.1.32437.1.5.1.4.3.0";
    private static final String NOT_FOUND_OID = "1.3.4.1.2.1.343";
    private static final String WALK_OID = "1.3.6.1.4.1.32437";
    private static final String READ_ONLY_OID_VALUE_1 = "TestOID1";
    private static final String READ_ONLY_OID_VALUE_2 = "TestOID2";
    private static final String GET = "GET";
    private static final String WALK = "WALK";

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

    private static Stream<Arguments> provideArguments() {
        return Stream.of(
                Arguments.of(v1TestAgent, v1TestRunnerFactory),
                Arguments.of(v2cTestAgent, v2cTestRunnerFactory),
                Arguments.of(v3TestAgent, v3TestRunnerFactory)
        );
    }

    @ParameterizedTest
    @MethodSource("provideArguments")
    void testSnmpGet(TestAgent testAgent, SNMPTestRunnerFactory testRunnerFactory) throws IOException {
        testAgent.start();
        try {
            final TestRunner runner = testRunnerFactory.createSnmpGetTestRunner(testAgent.getPort(), READ_ONLY_OID_1, GET);
            runner.run();
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).getFirst();

            assertNotNull(successFF);
            assertEquals(READ_ONLY_OID_VALUE_1, successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + READ_ONLY_OID_1 + SNMPUtils.SNMP_PROP_DELIMITER + "4"));
        } catch (Exception e) {
            fail(e);
        } finally {
            testAgent.stop();
            testAgent.unregister();
        }
    }

    @ParameterizedTest
    @MethodSource("provideArguments")
    void testSnmpWalk(TestAgent testAgent, SNMPTestRunnerFactory testRunnerFactory) throws IOException {
        testAgent.start();
        try {
            final TestRunner runner = testRunnerFactory.createSnmpGetTestRunner(testAgent.getPort(), WALK_OID, WALK);
            runner.run();
            final MockFlowFile successFF = runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).getFirst();
            assertNotNull(successFF);

            assertEquals(READ_ONLY_OID_VALUE_1, successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + READ_ONLY_OID_1 + SNMPUtils.SNMP_PROP_DELIMITER + "4"));
            assertEquals(READ_ONLY_OID_VALUE_2, successFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + READ_ONLY_OID_2 + SNMPUtils.SNMP_PROP_DELIMITER + "4"));
        } catch (Exception e) {
            fail(e);
        } finally {
            testAgent.stop();
            testAgent.unregister();
        }
    }

    @ParameterizedTest
    @MethodSource("provideArguments")
    void testSnmpGetWithEmptyResponse(TestAgent testAgent, SNMPTestRunnerFactory testRunnerFactory) throws IOException {
        testAgent.start();
        try {
            final MockFlowFile mockFlowFile = new MockFlowFile(0L);
            mockFlowFile.putAttributes(Collections.singletonMap("snmp$" + NOT_FOUND_OID, StringUtils.EMPTY));
            final TestRunner runner = testRunnerFactory.createSnmpGetTestRunner(testAgent.getPort(), NOT_FOUND_OID, GET);
            runner.enqueue(mockFlowFile);
            runner.run();

            if (testAgent == v1TestAgent) {
                final MockFlowFile failureFF = runner.getFlowFilesForRelationship(GetSNMP.REL_FAILURE).getFirst();
                assertNotNull(failureFF);
                assertEquals(StringUtils.EMPTY, failureFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + NOT_FOUND_OID));
                assertEquals("No such name", failureFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + "errorStatusText"));
            } else {
                final MockFlowFile failureFF = runner.getFlowFilesForRelationship(GetSNMP.REL_FAILURE).getFirst();
                assertNotNull(failureFF);
                assertEquals("noSuchObject", failureFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + NOT_FOUND_OID + SNMPUtils.SNMP_PROP_DELIMITER + "128"));
                assertEquals("Success", failureFF.getAttribute(SNMPUtils.SNMP_PROP_PREFIX + "errorStatusText"));
            }
        } catch (Exception e) {
            fail(e);
        } finally {
            testAgent.stop();
            testAgent.unregister();
        }
    }

    private static void registerManagedObjects(final TestAgent agent) {
        agent.registerManagedObjects(
                DefaultMOFactory.getInstance().createScalar(new OID(READ_ONLY_OID_1), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(READ_ONLY_OID_VALUE_1)),
                DefaultMOFactory.getInstance().createScalar(new OID(READ_ONLY_OID_2), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(READ_ONLY_OID_VALUE_2))
        );
    }
}
