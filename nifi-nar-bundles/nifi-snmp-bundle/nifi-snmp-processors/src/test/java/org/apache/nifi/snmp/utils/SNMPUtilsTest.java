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
package org.apache.nifi.snmp.utils;

import org.junit.Test;
import org.snmp4j.PDU;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link SNMPUtils}.
 */
public class SNMPUtilsTest {

    /**
     * Test for updating attributes of flow files with {@link PDU}
     */
    @Test
    public void validateUpdateFlowFileAttributes() {
        final PDU pdu = new PDU();
        pdu.setErrorIndex(0);
        pdu.setErrorStatus(0);
        pdu.setType(4);

        final Map<String, String> attributeMap = SNMPUtils.getPduAttributeMap(pdu);

        assertEquals("0", attributeMap.get(SNMPUtils.SNMP_PROP_PREFIX + "errorIndex"));
        assertEquals("0", attributeMap.get(SNMPUtils.SNMP_PROP_PREFIX + "errorStatus"));
        assertEquals("4", attributeMap.get(SNMPUtils.SNMP_PROP_PREFIX + "type"));
    }
}
