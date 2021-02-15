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

import org.apache.nifi.snmp.testagents.TestSNMPV1Agent;
import org.apache.nifi.snmp.testagents.TestSNMPV2Agent;
import org.apache.nifi.snmp.testagents.TestSNMPV3Agent;
import org.apache.nifi.snmp.utils.SNMPUtilsTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.ScopedPDU;
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

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SNMPSetterTest {

    private static TestSNMPV1Agent snmpV1Agent;
    private static TestSNMPV2Agent snmpV2Agent;
    private static TestSNMPV3Agent snmpV3Agent;
    private static final OID readOnlyOID = new OID("1.3.6.1.4.1.32437.1.5.1.4.2.0");
    private static final OID writeOnlyOID = new OID("1.3.6.1.4.1.32437.1.5.1.4.3.0");
    private static final String readOnlyOIDValue = "readOnlyOID";
    private static final String writeOnlyOIDValue = "writeOnlyOID";


    @BeforeClass
    public static void setUp() throws IOException {
        snmpV1Agent = new TestSNMPV1Agent("0.0.0.0");
        snmpV1Agent.start();
        snmpV1Agent.registerManagedObjects(
                DefaultMOFactory.getInstance().createScalar(new OID(readOnlyOID), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(readOnlyOIDValue)),
                DefaultMOFactory.getInstance().createScalar(new OID(writeOnlyOID), MOAccessImpl.ACCESS_WRITE_ONLY, new OctetString(writeOnlyOIDValue))
        );
        snmpV2Agent = new TestSNMPV2Agent("0.0.0.0");
        snmpV2Agent.start();
        snmpV2Agent.registerManagedObjects(
                DefaultMOFactory.getInstance().createScalar(new OID(readOnlyOID), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(readOnlyOIDValue)),
                DefaultMOFactory.getInstance().createScalar(new OID(writeOnlyOID), MOAccessImpl.ACCESS_WRITE_ONLY, new OctetString(writeOnlyOIDValue))
        );
        snmpV3Agent = new TestSNMPV3Agent("0.0.0.0");
        snmpV3Agent.start();
        snmpV3Agent.registerManagedObjects(
                DefaultMOFactory.getInstance().createScalar(new OID(readOnlyOID), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(readOnlyOIDValue)),
                DefaultMOFactory.getInstance().createScalar(new OID(writeOnlyOID), MOAccessImpl.ACCESS_WRITE_ONLY, new OctetString(writeOnlyOIDValue))
        );
    }

    @AfterClass
    public static void tearDown() {
        snmpV1Agent.stop();
        snmpV2Agent.stop();
        snmpV3Agent.stop();
    }

    @Test
    public void testSuccessfulSnmpV1Set() throws IOException, InterruptedException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + snmpV1Agent.getPort(), SnmpConstants.version1);
        String expectedOIDValue = "testValue";
        try (SNMPSetter setter = new SNMPSetter(snmp, target)) {
            PDU pdu = new PDU();
            pdu.add(new VariableBinding(writeOnlyOID, new OctetString(expectedOIDValue)));
            pdu.setType(PDU.SET);
            ResponseEvent response = setter.set(pdu);

            assertEquals(expectedOIDValue, response.getResponse().get(0).getVariable().toString());
        }
    }

    @Test
    public void testSuccessfulSnmpV2Set() throws IOException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/" + snmpV2Agent.getPort(), SnmpConstants.version2c);
        String expectedOIDValue = "testValue";
        try (SNMPSetter setter = new SNMPSetter(snmp, target)) {
            PDU pdu = new PDU();
            pdu.add(new VariableBinding(readOnlyOID, new OctetString(expectedOIDValue)));
            pdu.setType(PDU.SET);
            ResponseEvent response = setter.set(pdu);

            assertEquals(expectedOIDValue, response.getResponse().get(0).getVariable().toString());
        }
    }

    @Test
    public void testSuccessfulSnmpV3Set() throws IOException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        final UserTarget userTarget = SNMPUtilsTest.prepareUser(snmp, "127.0.0.1/" + snmpV3Agent.getPort(), SecurityLevel.AUTH_NOPRIV,
                "SHA", AuthSHA.ID, null, "SHAAuthPassword", null);
        String expectedOIDValue = "testValue";
        try (SNMPSetter setter = new SNMPSetter(snmp, userTarget)) {
            ScopedPDU pdu = new ScopedPDU();
            pdu.add(new VariableBinding(writeOnlyOID, new OctetString(expectedOIDValue)));
            pdu.setType(PDU.SET);
            ResponseEvent response = setter.set(pdu);

            assertEquals(expectedOIDValue, response.getResponse().get(0).getVariable().toString());
        }
    }

    @Test
    public void testCannotSetReadOnlyObject() throws IOException {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        final UserTarget userTarget = SNMPUtilsTest.prepareUser(snmp, "127.0.0.1/" + snmpV3Agent.getPort(), SecurityLevel.AUTH_NOPRIV,
                "SHA", AuthSHA.ID, null, "SHAAuthPassword", null);
        String expectedOIDValue = "testValue";
        try (SNMPSetter setter = new SNMPSetter(snmp, userTarget); SNMPGetter getter = new SNMPGetter(snmp, userTarget, readOnlyOID)) {
            ScopedPDU pdu = new ScopedPDU();
            pdu.add(new VariableBinding(readOnlyOID, new OctetString(expectedOIDValue)));
            pdu.setType(PDU.SET);
            ResponseEvent response = setter.set(pdu);

            assertEquals(expectedOIDValue, response.getResponse().get(0).getVariable().toString());

            final ResponseEvent responseEvent = getter.get();

            assertNotEquals(expectedOIDValue, responseEvent.getResponse().get(0).getVariable().toString());
            assertEquals(readOnlyOIDValue, responseEvent.getResponse().get(0).getVariable().toString());
        }
    }

}
