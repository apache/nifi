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
package org.apache.nifi.snmp.helper;

import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.UserTarget;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.io.IOException;

public class SNMPTestUtils {

    public static Snmp createSnmpClient() throws IOException {
        final DefaultUdpTransportMapping transportMapping = new DefaultUdpTransportMapping();
        transportMapping.listen();
        return new Snmp(transportMapping);
    }

    public static CommunityTarget createCommTarget(final String community, final String address, final int version) {
        final CommunityTarget target = new CommunityTarget();
        target.setVersion(version);
        target.setCommunity(new OctetString(community));
        target.setAddress(new UdpAddress(address));
        target.setRetries(0);
        target.setTimeout(500);
        return target;
    }

    public static UserTarget createUserTarget(final String address, final int securityLevel, final String securityName, final int version) {
        final UserTarget target = new UserTarget();
        target.setVersion(version);
        target.setSecurityLevel(securityLevel);
        target.setSecurityName(new OctetString(securityName));
        target.setAddress(new UdpAddress(address));
        target.setRetries(0);
        target.setTimeout(500);
        return target;
    }

    public static UserTarget prepareUser(final Snmp snmp, final int version, final String address, final int securityLevel, final String securityName,
                                         final OID auth, final OID priv, final String authPwd, final String privPwd) {
        snmp.getUSM().removeAllUsers();
        final OctetString aPwd = authPwd != null ? new OctetString(authPwd) : null;
        final OctetString pPwd = privPwd != null ? new OctetString(privPwd) : null;
        snmp.getUSM().addUser(new OctetString(securityName), new UsmUser(new OctetString(securityName), auth, aPwd, priv, pPwd));
        return createUserTarget(address, securityLevel, securityName, version);
    }

}
