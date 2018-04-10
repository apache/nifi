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
 * Kerberos configuration class from {@link org.apache.hadoop.security.authentication.client.KerberosAuthenticator.KerberosConfiguration}.
 */
public class KerberosConfiguration extends javax.security.auth.login.Configuration {

    private static final boolean IBM_JAVA = System.getProperty("java.vendor").contains("IBM");
    private static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");
    private static final boolean IS_64_BIT = System.getProperty("os.arch").contains("64");
    private static final boolean AIX = System.getProperty("os.name").equals("AIX");
    private static final Map<String, String> USER_KERBEROS_OPTIONS = new HashMap<>();
    private static final String OS_LOGIN_MODULE_NAME;
    private static final AppConfigurationEntry OS_SPECIFIC_LOGIN;
    private static final AppConfigurationEntry USER_KERBEROS_LOGIN;
    private static final AppConfigurationEntry[] USER_KERBEROS_CONF;

    KerberosConfiguration(String principal, String keytab) {
        USER_KERBEROS_OPTIONS.put("principal", principal);
        USER_KERBEROS_OPTIONS.put("keyTab", keytab);
    }

    private static String getOSLoginModuleName() {
        if (IBM_JAVA) {
            if (WINDOWS) {
                return IS_64_BIT ? "com.ibm.security.auth.module.Win64LoginModule" : "com.ibm.security.auth.module.NTLoginModule";
            } else if (AIX) {
                return IS_64_BIT ? "com.ibm.security.auth.module.AIX64LoginModule" : "com.ibm.security.auth.module.AIXLoginModule";
            } else {
                return "com.ibm.security.auth.module.LinuxLoginModule";
            }
        } else {
            return WINDOWS ? "com.sun.security.auth.module.NTLoginModule" : "com.sun.security.auth.module.UnixLoginModule";
        }
    }

    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
        return USER_KERBEROS_CONF;
    }

    static {
        USER_KERBEROS_OPTIONS.put("doNotPrompt", "true");
        USER_KERBEROS_OPTIONS.put("useTicketCache", "true");
        USER_KERBEROS_OPTIONS.put("renewTGT", "true");
        USER_KERBEROS_OPTIONS.put("useKeyTab", "true");
        USER_KERBEROS_OPTIONS.put("storeKey", "true");
        USER_KERBEROS_OPTIONS.put("refreshKrb5Config", "true");
        OS_LOGIN_MODULE_NAME = getOSLoginModuleName();
        OS_SPECIFIC_LOGIN = new AppConfigurationEntry(OS_LOGIN_MODULE_NAME, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, new HashMap<>());
        USER_KERBEROS_LOGIN = new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL, USER_KERBEROS_OPTIONS);
        USER_KERBEROS_CONF = new AppConfigurationEntry[]{OS_SPECIFIC_LOGIN, USER_KERBEROS_LOGIN};
    }

}
