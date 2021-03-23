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

import org.apache.nifi.snmp.dto.SNMPSingleResponse;
import org.apache.nifi.snmp.dto.SNMPTreeResponse;
import org.apache.nifi.snmp.helper.SNMPTestUtils;
import org.apache.nifi.snmp.testagents.TestAgent;
import org.apache.nifi.util.MockFlowFile;
import org.junit.After;
import org.junit.Before;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.agent.mo.DefaultMOFactory;
import org.snmp4j.agent.mo.MOAccessImpl;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class SNMPRequestTest {

    protected static final String LOCALHOST = "127.0.0.1";
    protected static final String INVALID_HOST = "127.0.0.2";
    protected static final String READ_ONLY_OID_1 = "1.3.6.1.4.1.32437.1.5.1.4.2.0";
    protected static final String READ_ONLY_OID_2 = "1.3.6.1.4.1.32437.1.5.1.4.3.0";
    protected static final String WRITE_ONLY_OID = "1.3.6.1.4.1.32437.1.5.1.4.4.0";
    protected static final String READ_ONLY_OID_VALUE_1 = "TestOID1";
    protected static final String READ_ONLY_OID_VALUE_2 = "TestOID2";
    protected static final String WRITE_ONLY_OID_VALUE = "writeOnlyOID";
    protected static final String SNMP_PROP_DELIMITER = "$";
    protected static final String SNMP_PROP_PREFIX = "snmp" + SNMP_PROP_DELIMITER;
    protected static final String NOT_WRITABLE = "Not writable";
    protected static final String NO_ACCESS = "No access";
    protected static final String SUCCESS = "Success";
    protected static final String EXPECTED_OID_VALUE = "testValue";
    protected static final Map<String, String> WALK_OID_MAP;

    static {
        final Map<String, String> oidMap = new HashMap<>();
        oidMap.put(READ_ONLY_OID_1, READ_ONLY_OID_VALUE_1);
        oidMap.put(READ_ONLY_OID_2, READ_ONLY_OID_VALUE_2);
        WALK_OID_MAP = Collections.unmodifiableMap(oidMap);
    }

    protected final TestAgent agent = getAgentInstance();

    protected abstract TestAgent getAgentInstance();

    @Before
    public void initAgent() throws IOException {
        agent.start();
        agent.registerManagedObjects(
                DefaultMOFactory.getInstance().createScalar(new OID(READ_ONLY_OID_1), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(READ_ONLY_OID_VALUE_1)),
                DefaultMOFactory.getInstance().createScalar(new OID(READ_ONLY_OID_2), MOAccessImpl.ACCESS_READ_ONLY, new OctetString(READ_ONLY_OID_VALUE_2)),
                DefaultMOFactory.getInstance().createScalar(new OID(WRITE_ONLY_OID), MOAccessImpl.ACCESS_WRITE_ONLY, new OctetString(WRITE_ONLY_OID_VALUE))
        );
    }

    @After
    public void tearDown() {
        agent.stop();
    }

    protected SNMPTreeResponse getTreeEvents(final int port, final int version) throws IOException {
        final Snmp snmp = SNMPTestUtils.createSnmpClient();
        final CommunityTarget target = SNMPTestUtils.createCommTarget("public", LOCALHOST + "/" + port, version);
        final StandardSNMPRequestHandler standardSnmpRequestHandler = new StandardSNMPRequestHandler(snmp, target);
        return standardSnmpRequestHandler.walk("1.3.6.1.4.1.32437");
    }

    protected SNMPSingleResponse getResponseEvent(final String address, final int port, final int version, final String oid) throws IOException {
        final Snmp snmp = SNMPTestUtils.createSnmpClient();
        final CommunityTarget target = SNMPTestUtils.createCommTarget("public", address + "/" + port, version);
        final StandardSNMPRequestHandler standardSnmpRequestHandler = new StandardSNMPRequestHandler(snmp, target);
        return standardSnmpRequestHandler.get(oid);
    }

    protected SNMPSingleResponse getSetResponse(final int port, final int version, final String oid, final String expectedOid) throws IOException {
        final Snmp snmp = SNMPTestUtils.createSnmpClient();
        final CommunityTarget target = SNMPTestUtils.createCommTarget("public", LOCALHOST + "/" + port, version);
        final StandardSNMPRequestHandler standardSnmpRequestHandler = new StandardSNMPRequestHandler(snmp, target);

        final MockFlowFile flowFile = new MockFlowFile(1L);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(SNMP_PROP_PREFIX + oid, expectedOid);
        flowFile.putAttributes(attributes);

        return standardSnmpRequestHandler.set(flowFile);
    }

    protected void assertSubTreeContainsOids(SNMPTreeResponse response) {
        final Map<String, String> attributes = response.getAttributes();
        attributes.entrySet().forEach(this::checkEntryContainsSubString);
    }

    private void checkEntryContainsSubString(Map.Entry<String, String> attribute) {
        final AtomicBoolean isMatch = new AtomicBoolean(false);
        WALK_OID_MAP.forEach((key, value) -> {
            if (!isMatch.get() && attribute.getKey().contains(key)) {
                isMatch.set(true);
                assertEquals(value, attribute.getValue());
            }
        });
        if (!isMatch.get()) {
            fail("Expected OID did not found in subtree.");
        }
    }
}
