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

import org.junit.Test;

import javax.security.auth.login.AppConfigurationEntry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestKeytabConfiguration {

    @Test
    public void testCreatingKeytabConfiguration() {
        final String principal = "foo@NIFI.COM";
        final String keytab = "src/test/resources/foo.keytab";

        final KeytabConfiguration configuration = new KeytabConfiguration(principal, keytab);
        assertEquals(principal, configuration.getPrincipal());
        assertEquals(keytab, configuration.getKeytabFile());

        final AppConfigurationEntry[] entries = configuration.getAppConfigurationEntry("KeytabConfig");
        assertNotNull(entries);
        assertEquals(1, entries.length);

        final AppConfigurationEntry entry = entries[0];
        assertEquals(ConfigurationUtil.SUN_KRB5_LOGIN_MODULE, entry.getLoginModuleName());
        assertEquals(principal, entry.getOptions().get("principal"));
        assertEquals(keytab, entry.getOptions().get("keyTab"));
    }

}
