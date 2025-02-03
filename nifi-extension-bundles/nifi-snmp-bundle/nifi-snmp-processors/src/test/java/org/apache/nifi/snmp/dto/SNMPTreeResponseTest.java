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
package org.apache.nifi.snmp.dto;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Target;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.util.TreeEvent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SNMPTreeResponseTest {

    private static final String SNMP_SEPARATOR = "$";
    private static final String SNMP_PREFIX = "snmp" + SNMP_SEPARATOR;
    private static final String VB_SYNTAX = "4";

    private static final String OID_1 = "1.3.6.1.4.1.32437.1.5.1.4.2.0";
    private static final String OID_2 = "1.3.6.1.4.1.32437.1.5.1.4.3.0";
    private static final String OID_3 = "1.3.6.1.4.1.32437.1.5.1.4.4.0";
    private static final String OID_1_VALUE = "OID_1_VALUE";
    private static final String OID_2_VALUE = "OID_2_VALUE";
    private static final String OID_3_VALUE = "OID_3_VALUE";

    private static final String TARGET_ADDRESS = "127.0.0.1/50555";

    private static VariableBinding[] vbs1;
    private static VariableBinding[] vbs2;
    private static VariableBinding[] vbs3;
    private static Map<String, String> vbMap;
    @Mock
    private static Target<UdpAddress> target;

    @BeforeAll
    public static void setUp() {
        vbMap = new HashMap<>();
        vbMap.put(SNMP_PREFIX + OID_1 + SNMP_SEPARATOR + VB_SYNTAX, OID_1_VALUE);
        vbMap.put(SNMP_PREFIX + OID_2 + SNMP_SEPARATOR + VB_SYNTAX, OID_2_VALUE);
        vbMap.put(SNMP_PREFIX + OID_3 + SNMP_SEPARATOR + VB_SYNTAX, OID_3_VALUE);

        target = new CommunityTarget<>();
        target.setAddress(new UdpAddress(TARGET_ADDRESS));

        vbs1 = new VariableBinding[]{
                new VariableBinding(new OID(OID_1), new OctetString(OID_1_VALUE)),
        };
        vbs2 = new VariableBinding[]{
                new VariableBinding(new OID(OID_2), new OctetString(OID_2_VALUE)),
        };
        vbs3 = new VariableBinding[]{
                new VariableBinding(new OID(OID_3), new OctetString(OID_3_VALUE))
        };
    }

    @Test
    void testGetAttributes() {
        final TreeEvent treeEvent1 = mock(TreeEvent.class);
        when(treeEvent1.getVariableBindings()).thenReturn(vbs1);
        final TreeEvent treeEvent2 = mock(TreeEvent.class);
        when(treeEvent2.getVariableBindings()).thenReturn(vbs2);
        final TreeEvent treeEvent3 = mock(TreeEvent.class);
        when(treeEvent3.getVariableBindings()).thenReturn(vbs3);

        final List<TreeEvent> treeEvents = new ArrayList<>();
        Collections.addAll(treeEvents, treeEvent1, treeEvent2, treeEvent3);

        final SNMPTreeResponse snmpTreeResponse = new SNMPTreeResponse(target, treeEvents);
        final Map<String, String> attributes = snmpTreeResponse.getAttributes();

        assertEquals(3, attributes.size());
        assertEquals(vbMap, attributes);
    }

    @Test
    void testGetAttributesFlattensEmptyVariableBindingArrays() {
        final TreeEvent emptyTreeEvent = mock(TreeEvent.class);
        when(emptyTreeEvent.getVariableBindings()).thenReturn(new VariableBinding[0]);
        final TreeEvent normalTreeEvent = mock(TreeEvent.class);
        when(normalTreeEvent.getVariableBindings()).thenReturn(vbs1);

        final List<TreeEvent> treeEvents = new ArrayList<>();
        Collections.addAll(treeEvents, emptyTreeEvent, normalTreeEvent);

        final SNMPTreeResponse snmpTreeResponse = new SNMPTreeResponse(target, treeEvents);
        final Map<String, String> attributes = snmpTreeResponse.getAttributes();

        assertEquals(1, attributes.size());
        assertEquals(OID_1_VALUE, attributes.get(SNMP_PREFIX + OID_1 + SNMP_SEPARATOR + VB_SYNTAX));
    }

    @Test
    void testGetAttributesFiltersNullVariableBindings() {
        final TreeEvent nullTreeEvent = mock(TreeEvent.class);
        when(nullTreeEvent.getVariableBindings()).thenReturn(null);
        final TreeEvent normalTreeEvent = mock(TreeEvent.class);
        when(normalTreeEvent.getVariableBindings()).thenReturn(vbs1);

        final List<TreeEvent> treeEvents = new ArrayList<>();
        Collections.addAll(treeEvents, nullTreeEvent, normalTreeEvent);

        final SNMPTreeResponse snmpTreeResponse = new SNMPTreeResponse(target, treeEvents);
        final Map<String, String> attributes = snmpTreeResponse.getAttributes();

        assertEquals(1, attributes.size());
        assertEquals(OID_1_VALUE, attributes.get(SNMP_PREFIX + OID_1 + SNMP_SEPARATOR + VB_SYNTAX));
    }

    @Test
    void testGetTargetAddress() {
        final TreeEvent treeEvent = mock(TreeEvent.class);
        final List<TreeEvent> treeEvents = new ArrayList<>();
        treeEvents.add(treeEvent);
        final SNMPTreeResponse snmpTreeResponse = new SNMPTreeResponse(target, treeEvents);
        final String actualTargetAddress = snmpTreeResponse.getTargetAddress();

        assertEquals(TARGET_ADDRESS, actualTargetAddress);
    }
}
