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
import org.snmp4j.mp.SnmpConstants;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class SNMPFactoryProviderTest {

    @Test
    void testCreateFactoryByVersion() {
        final SNMPContext snmpV1V2cFactoryFromVersion1 = SNMPFactoryProvider.getFactory(SnmpConstants.version1);
        final SNMPContext snmpV1V2cFactoryFromVersion2c = SNMPFactoryProvider.getFactory(SnmpConstants.version2c);
        final SNMPContext snmpV3Factory = SNMPFactoryProvider.getFactory(SnmpConstants.version3);
        assertInstanceOf(V1V2cSNMPFactory.class, snmpV1V2cFactoryFromVersion1);
        assertInstanceOf(V1V2cSNMPFactory.class, snmpV1V2cFactoryFromVersion2c);
        assertInstanceOf(V3SNMPFactory.class, snmpV3Factory);
    }

}
