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
package org.apache.nifi.security.krb;

import org.junit.jupiter.api.Test;

import javax.security.auth.login.AppConfigurationEntry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestKerberosTicketCacheUser {

    @Test
    public void testGetConfigurationEntry() {
        final String principal = "foo@NIFI.COM";

        final KerberosUser kerberosUser = new KerberosTicketCacheUser(principal);
        assertEquals(principal, kerberosUser.getPrincipal());

        final AppConfigurationEntry entry = kerberosUser.getConfigurationEntry();
        assertNotNull(entry);
        assertEquals(ConfigurationUtil.SUN_KRB5_LOGIN_MODULE, entry.getLoginModuleName());
        assertEquals(AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, entry.getControlFlag());
        assertEquals(principal, entry.getOptions().get("principal"));
        assertEquals("true", entry.getOptions().get("useTicketCache"));
        assertNull(entry.getOptions().get("ticketCache"));
    }

    @Test
    public void testGetConfigurationEntryWithSpecificTicketCache() {
        final String principal = "foo@NIFI.COM";
        final String ticketCache = "/tmp/cache";

        final KerberosUser kerberosUser = new KerberosTicketCacheUser(principal, ticketCache);
        assertEquals(principal, kerberosUser.getPrincipal());

        final AppConfigurationEntry entry = kerberosUser.getConfigurationEntry();
        assertNotNull(entry);
        assertEquals(ConfigurationUtil.SUN_KRB5_LOGIN_MODULE, entry.getLoginModuleName());
        assertEquals(AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, entry.getControlFlag());
        assertEquals(principal, entry.getOptions().get("principal"));
        assertEquals(ticketCache, entry.getOptions().get("ticketCache"));
        assertEquals("true", entry.getOptions().get("useTicketCache"));

    }

}
