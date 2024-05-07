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
package org.apache.nifi.snmp.factory.core;

import org.junit.jupiter.api.Test;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.security.SecurityModels;
import org.snmp4j.security.USM;
import org.snmp4j.smi.Integer32;
import org.snmp4j.smi.OctetString;

import static org.apache.nifi.snmp.helper.configurations.SNMPV3ConfigurationFactory.SECURITY_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class V3SNMPFactoryTest extends SNMPSocketSupport {

    private static final int EXPECTED_SECURITY_LEVEL = 3;

    @Test
    void testFactoryCreatesTarget() {
        final V3SNMPFactory snmpFactory = new V3SNMPFactory();
        final Target target = createInstanceWithRetries(snmpFactory::createTargetInstance, 5);

        assertNotNull(target.getAddress().toString());
        assertEquals(RETRIES, target.getRetries());
        assertEquals(EXPECTED_SECURITY_LEVEL, target.getSecurityLevel());
        assertEquals(SECURITY_NAME, target.getSecurityName().toString());
    }

    @Test
    void testFactoryCreatesSnmpManager() {
        final V3SNMPFactory snmpFactory = new V3SNMPFactory();
        final Snmp snmpManager = createInstanceWithRetries(snmpFactory::createSnmpManagerInstance, 5);
        final String address = snmpManager.getMessageDispatcher().getTransportMappings().iterator().next().getListenAddress().toString();
        USM usm = (USM) SecurityModels.getInstance().getSecurityModel(new Integer32(3));
        assertNotNull(address);
        assertTrue(usm.hasUser(null, new OctetString("SHAAES128")));
    }
}
