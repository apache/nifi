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
package org.apache.nifi.snmp.operations;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.snmp.helper.SNMPTestUtil;
import org.apache.nifi.snmp.testagents.TestSNMPV1Agent;
import org.apache.nifi.snmp.testagents.TestSNMPV2Agent;
import org.apache.nifi.snmp.testagents.TestSNMPV3Agent;
import org.apache.nifi.snmp.utils.SNMPUtilsTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.snmp4j.CommunityTarget;
import org.snmp4j.SNMP4JSettings;
import org.snmp4j.Snmp;
import org.snmp4j.UserTarget;
import org.snmp4j.agent.mo.DefaultMOFactory;
import org.snmp4j.agent.mo.MOAccessImpl;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.AuthSHA;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.util.TreeEvent;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SNMPGetterTest {

    private static TestSNMPV1Agent snmpV1Agent;
    private static TestSNMPV2Agent snmpV2Agent;
    private static TestSNMPV3Agent snmpV3Agent;
    private static final OID readOnlyOID1 = new OID("1.3.6.1.4.1.32437.1.5.1.4.2.0");
    private static final OID readOnlyOID2 = new OID("1.3.6.1.4.1.32437.1.5.1.4.3.0");
    private static final String OIDValue1 = "TestOID1";
    private static final String OIDValue2 = "TestOID2";


    @BeforeClass
    public static void setUp() throws IOException {
        snmpV1Agent = new TestSNMPV1Agent("0.0.0.0");
        snmpV1Agent.start();
        snmpV1Agent.registerManagedObjects(
                DefaultMOFactory.getInstance().createScalar(new OID(readOnlyOID1), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(OIDValue1)),
                DefaultMOFactory.getInstance().createScalar(new OID(readOnlyOID2), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(OIDValue2))
        );
        snmpV2Agent = new TestSNMPV2Agent("0.0.0.0");
        snmpV2Agent.start();
        snmpV2Agent.registerManagedObjects(
                DefaultMOFactory.getInstance().createScalar(new OID(readOnlyOID1), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(OIDValue1)),
                DefaultMOFactory.getInstance().createScalar(new OID(readOnlyOID2), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(OIDValue2))
        );
        snmpV3Agent = new TestSNMPV3Agent("0.0.0.0");
        snmpV3Agent.start();
        snmpV3Agent.registerManagedObjects(
                DefaultMOFactory.getInstance().createScalar(new OID(readOnlyOID1), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(OIDValue1))
        );
    }


    @AfterClass
    public static void tearDown() {
        snmpV1Agent.stop();
        snmpV2Agent.stop();
        snmpV3Agent.stop();
    }

    @Test
    public void testSuccessfulSnmpV1Get() throws IOException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + snmpV1Agent.getPort(), SnmpConstants.version1);
        try (SNMPGetter getter = new SNMPGetter(snmp, target, readOnlyOID1)) {
            ResponseEvent response = getter.get();
            assertEquals(OIDValue1, response.getResponse().get(0).getVariable().toString());
        }
    }

    @Test
    public void testSuccessfulSnmpV1Walk() throws IOException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + snmpV1Agent.getPort(), SnmpConstants.version1);
        try (SNMPGetter getter = new SNMPGetter(snmp, target, new OID("1.3.6.1.4.1.32437"))) {
            final List<TreeEvent> responseEvents = getter.walk();
            assertEquals(OIDValue1, responseEvents.get(0).getVariableBindings()[0].getVariable().toString());
            assertEquals(OIDValue2, responseEvents.get(1).getVariableBindings()[0].getVariable().toString());
        }
    }

    @Test
    public void testSuccessfulSnmpV2Get() throws IOException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + snmpV2Agent.getPort(), SnmpConstants.version2c);
        try (SNMPGetter getter = new SNMPGetter(snmp, target, readOnlyOID1)) {
            ResponseEvent response = getter.get();
            assertEquals(OIDValue1, response.getResponse().get(0).getVariable().toString());
        }
    }

    @Test
    public void testSuccessfulSnmpV2Walk() throws IOException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + snmpV2Agent.getPort(), SnmpConstants.version2c);
        try (SNMPGetter getter = new SNMPGetter(snmp, target, new OID("1.3.6.1.4.1.32437"))) {
            final List<TreeEvent> responseEvents = getter.walk();
            final VariableBinding[] variableBindings = responseEvents.get(0).getVariableBindings();
            assertEquals(OIDValue1, variableBindings[0].getVariable().toString());
            assertEquals(OIDValue2, variableBindings[1].getVariable().toString());
        }
    }

    @Test
    public void testSuccessfulSnmpV3Get() throws IOException {
        SNMP4JSettings.setForwardRuntimeExceptions(true);
        Snmp snmp = SNMPUtilsTest.createSnmp();
        final UserTarget userTarget = SNMPUtilsTest.prepareUser(snmp, "127.0.0.1/" + snmpV3Agent.getPort(), SecurityLevel.AUTH_NOPRIV,
                "SHA", AuthSHA.ID, null, "SHAAuthPassword", null);
        try (SNMPGetter getter = new SNMPGetter(snmp, userTarget, readOnlyOID1)) {
            ResponseEvent response = getter.get();
            assertEquals(OIDValue1, response.getResponse().get(0).getVariable().toString());
        }
    }

    @Test
    public void testUnauthorizedUserSnmpV3GetReturnsNull() throws IOException {
        SNMP4JSettings.setForwardRuntimeExceptions(true);
        Snmp snmp = SNMPUtilsTest.createSnmp();
        final UserTarget userTarget = SNMPUtilsTest.prepareUser(snmp, "127.0.0.1/" + snmpV3Agent.getPort(), SecurityLevel.AUTH_NOPRIV,
                "FakeUserName", AuthSHA.ID, null, "FakeAuthPassword", null);
        try (SNMPGetter getter = new SNMPGetter(snmp, userTarget, readOnlyOID1)) {
            ResponseEvent response = getter.get();
            assertEquals("Null", response.getResponse().get(0).getVariable().toString());
        }
    }

    @Test
    public void testSnmpV1GetTimeoutReturnsNull() throws IOException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "1.2.3.4/" + SNMPTestUtil.availablePort(), SnmpConstants.version1);
        try (SNMPGetter getter = new SNMPGetter(snmp, target, readOnlyOID1)) {
            ResponseEvent response = getter.get();
            assertNull(response.getResponse());
        }
    }

    @Test(expected = ProcessException.class)
    public void testSnmpV1GetWithInvalidTargetThrowsException() throws IOException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + snmpV1Agent.getPort(), -1);
        try (SNMPGetter getter = new SNMPGetter(snmp, target, readOnlyOID1)) {
            getter.get();
        }
    }
}
