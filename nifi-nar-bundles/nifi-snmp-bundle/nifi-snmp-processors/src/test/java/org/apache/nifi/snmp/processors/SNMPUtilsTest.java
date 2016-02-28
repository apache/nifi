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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.junit.Test;
import org.snmp4j.CommunityTarget;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.UserTarget;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.transport.DefaultUdpTransportMapping;

/**
 * Test class for {@link SNMPUtils}.
 */
public class SNMPUtilsTest {

    /**
     * Test for updating attributes of flow files with {@link PDU}
     */
    @Test
    public void validateUpdateFlowFileAttributes() {
        GetSNMP processor = new GetSNMP();
        ProcessSession processSession = new MockProcessSession(new SharedSessionState(processor, new AtomicLong()),
                processor);
        FlowFile sourceFlowFile = processSession.create();

        PDU pdu = new PDU();
        pdu.setErrorIndex(0);
        pdu.setErrorStatus(0);
        pdu.setType(4);

        FlowFile f2 = SNMPUtils.updateFlowFileAttributesWithPduProperties(pdu, sourceFlowFile,
                processSession);

        assertEquals("0", f2.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + "errorIndex"));
        assertEquals("0", f2.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + "errorStatus"));
        assertEquals("4", f2.getAttributes().get(SNMPUtils.SNMP_PROP_PREFIX + "type"));
    }

    /**
     * Method to create an instance of SNMP
     * @return instance of SNMP
     * @throws IOException IO Exception
     */
    protected static Snmp createSnmp() throws IOException {
        DefaultUdpTransportMapping transportMapping = new DefaultUdpTransportMapping();
        transportMapping.listen();
        return new Snmp(transportMapping);
    }

    /**
     * Method to create community target
     * @param community community name
     * @param address address
     * @param version SNMP version
     * @return community target
     */
    protected static CommunityTarget createCommTarget(String community, String address, int version) {
        CommunityTarget target = new CommunityTarget();
        target.setVersion(version);
        target.setCommunity(new OctetString(community));
        target.setAddress(new UdpAddress(address));
        target.setRetries(0);
        target.setTimeout(500);
        return target;
    }

    /**
     * Method to create a user target
     * @param address address
     * @param securityLevel security level
     * @param securityName security name
     * @return user target
     */
    protected static UserTarget createUserTarget(String address, int securityLevel, String securityName) {
        UserTarget target = new UserTarget();
        target.setVersion(SnmpConstants.version3);
        target.setSecurityLevel(securityLevel);
        target.setSecurityName(new OctetString(securityName));
        target.setAddress(new UdpAddress("127.0.0.1/2003"));
        target.setRetries(0);
        target.setTimeout(500);
        return target;
    }

    /**
     * Method to prepare user target and to add id in the User Based Security Model of the given SNMP instance
     * @param snmp SNMP instance
     * @param address address
     * @param securityLevel security level
     * @param securityName security name
     * @param auth authentication protocol
     * @param priv private protocol
     * @param authPwd authentication password
     * @param privPwd private password
     * @return user target
     */
    protected static UserTarget prepareUser(Snmp snmp, String address, int securityLevel, String securityName, OID auth, OID priv, String authPwd, String privPwd) {
        snmp.getUSM().removeAllUsers();
        OctetString aPwd = authPwd != null ? new OctetString(authPwd) : null;
        OctetString pPwd = privPwd != null ? new OctetString(privPwd) : null;
        snmp.getUSM().addUser(new OctetString(securityName), new UsmUser(new OctetString(securityName), auth, aPwd, priv, pPwd));
        return createUserTarget(address, securityLevel, securityName);
    }

}
