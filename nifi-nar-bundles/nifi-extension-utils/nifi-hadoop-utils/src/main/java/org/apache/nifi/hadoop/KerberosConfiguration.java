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
package org.apache.nifi.hadoop;

import org.apache.hadoop.security.authentication.util.KerberosUtil;

import javax.security.auth.login.AppConfigurationEntry;
import java.util.HashMap;
import java.util.Map;

/**
 * Modified Kerberos configuration class from {@link org.apache.hadoop.security.authentication.client.KerberosAuthenticator.KerberosConfiguration}
 * that requires authentication from a keytab.
 */
public class KerberosConfiguration extends javax.security.auth.login.Configuration {

    private static final Map<String, String> USER_KERBEROS_OPTIONS = new HashMap<>();
    private static final AppConfigurationEntry USER_KERBEROS_LOGIN;
    private static final AppConfigurationEntry[] USER_KERBEROS_CONF;

    KerberosConfiguration(String principal, String keytab) {
        USER_KERBEROS_OPTIONS.put("principal", principal);
        USER_KERBEROS_OPTIONS.put("keyTab", keytab);
    }

    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
        return USER_KERBEROS_CONF;
    }

    static {
        USER_KERBEROS_OPTIONS.put("doNotPrompt", "true");
        USER_KERBEROS_OPTIONS.put("useKeyTab", "true");
        USER_KERBEROS_OPTIONS.put("refreshKrb5Config", "true");
        USER_KERBEROS_LOGIN = new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, USER_KERBEROS_OPTIONS);
        USER_KERBEROS_CONF = new AppConfigurationEntry[]{USER_KERBEROS_LOGIN};
    }

}
