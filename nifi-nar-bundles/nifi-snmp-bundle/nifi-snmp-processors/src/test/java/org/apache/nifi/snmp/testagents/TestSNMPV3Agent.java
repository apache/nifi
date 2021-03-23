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
package org.apache.nifi.snmp.testagents;

import org.snmp4j.agent.CommandProcessor;
import org.snmp4j.agent.mo.snmp.StorageType;
import org.snmp4j.agent.mo.snmp.VacmMIB;
import org.snmp4j.agent.security.MutableVACM;
import org.snmp4j.mp.MPv3;
import org.snmp4j.security.AuthMD5;
import org.snmp4j.security.AuthSHA;
import org.snmp4j.security.PrivAES128;
import org.snmp4j.security.PrivDES;
import org.snmp4j.security.SecurityLevel;
import org.snmp4j.security.SecurityModel;
import org.snmp4j.security.USM;
import org.snmp4j.security.UsmUser;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;

import java.io.File;
import java.util.UUID;

public class TestSNMPV3Agent extends TestAgent {

    public TestSNMPV3Agent(final String host) {
        super(new File(String.format(BOOT_COUNTER_NAME_TEMPLATE, 1, UUID.randomUUID())), new File(String.format(CONFIG_NAME_TEMPLATE, 1, UUID.randomUUID())),
                new CommandProcessor(new OctetString(MPv3.createLocalEngineID())), host);
    }

    @Override
    protected void addUsmUser(final USM usm) {
        UsmUser user = new UsmUser(new OctetString("SHA"),
                AuthSHA.ID,
                new OctetString("SHAAuthPassword"),
                null,
                null);
        usm.addUser(user.getSecurityName(), usm.getLocalEngineID(), user);
        user = new UsmUser(new OctetString("SHADES"),
                AuthSHA.ID,
                new OctetString("SHADESAuthPassword"),
                PrivDES.ID,
                new OctetString("SHADESPrivPassword"));
        usm.addUser(user.getSecurityName(), usm.getLocalEngineID(), user);
        user = new UsmUser(new OctetString("MD5DES"),
                AuthMD5.ID,
                new OctetString("MD5DESAuthPassword"),
                PrivDES.ID,
                new OctetString("MD5DESPrivPassword"));
        usm.addUser(user.getSecurityName(), usm.getLocalEngineID(), user);
        user = new UsmUser(new OctetString("SHAAES128"),
                AuthSHA.ID,
                new OctetString("SHAAES128AuthPassword"),
                PrivAES128.ID,
                new OctetString("SHAAES128PrivPassword"));
        usm.addUser(user.getSecurityName(), usm.getLocalEngineID(), user);
    }

    @Override
    protected void addViews(final VacmMIB vacmMIB) {
        vacmMIB.addGroup(SecurityModel.SECURITY_MODEL_USM,
                new OctetString("SHA"),
                new OctetString("v3-auth-no-priv-group"),
                StorageType.nonVolatile);
        vacmMIB.addGroup(SecurityModel.SECURITY_MODEL_USM,
                new OctetString("SHADES"),
                new OctetString("v3group"),
                StorageType.nonVolatile);
        vacmMIB.addGroup(SecurityModel.SECURITY_MODEL_USM,
                new OctetString("MD5DES"),
                new OctetString("v3group"),
                StorageType.nonVolatile);
        vacmMIB.addGroup(SecurityModel.SECURITY_MODEL_USM,
                new OctetString("SHAAES128"),
                new OctetString("v3group"),
                StorageType.nonVolatile);
        vacmMIB.addAccess(new OctetString("v3group"), new OctetString(),
                SecurityModel.SECURITY_MODEL_USM,
                SecurityLevel.AUTH_PRIV,
                MutableVACM.VACM_MATCH_EXACT,
                new OctetString("fullReadView"),
                new OctetString("fullWriteView"),
                new OctetString("fullNotifyView"),
                StorageType.nonVolatile);
        vacmMIB.addAccess(new OctetString("v3-auth-no-priv-group"), new OctetString(),
                SecurityModel.SECURITY_MODEL_USM,
                SecurityLevel.AUTH_NOPRIV,
                MutableVACM.VACM_MATCH_EXACT,
                new OctetString("fullReadView"),
                new OctetString("fullWriteView"),
                new OctetString("fullNotifyView"),
                StorageType.nonVolatile);
        vacmMIB.addViewTreeFamily(new OctetString("fullReadView"), new OID("1.3"),
                new OctetString(), VacmMIB.vacmViewIncluded,
                StorageType.nonVolatile);
        vacmMIB.addViewTreeFamily(new OctetString("fullWriteView"), new OID("1.3"),
                new OctetString(), VacmMIB.vacmViewIncluded,
                StorageType.nonVolatile);
        vacmMIB.addViewTreeFamily(new OctetString("fullNotifyView"), new OID("1.3"),
                new OctetString(), VacmMIB.vacmViewIncluded,
                StorageType.nonVolatile);
    }
}
