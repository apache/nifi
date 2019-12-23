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

import org.apache.commons.lang3.StringUtils;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom JAAS Configuration object for a provided principal and keytab.
 */
public class KeytabConfiguration extends Configuration {

    private final String principal;
    private final String keytabFile;

    private final AppConfigurationEntry kerberosKeytabConfigEntry;

    public KeytabConfiguration(final String principal, final String keytabFile) {
        if (StringUtils.isBlank(principal)) {
            throw new IllegalArgumentException("Principal cannot be null");
        }

        if (StringUtils.isBlank(keytabFile)) {
            throw new IllegalArgumentException("Keytab file cannot be null");
        }

        this.principal = principal;
        this.keytabFile = keytabFile;

        final Map<String, String> options = new HashMap<>();
        options.put("principal", principal);
        options.put("refreshKrb5Config", "true");

        if (ConfigurationUtil.IS_IBM) {
            options.put("useKeytab", keytabFile);
            options.put("credsType", "both");
        } else {
            options.put("keyTab", keytabFile);
            options.put("useKeyTab", "true");
            options.put("isInitiator", "true");
            options.put("doNotPrompt", "true");
            options.put("storeKey", "true");
        }

        final String krbLoginModuleName = ConfigurationUtil.IS_IBM
                ? ConfigurationUtil.IBM_KRB5_LOGIN_MODULE : ConfigurationUtil.SUN_KRB5_LOGIN_MODULE;

        this.kerberosKeytabConfigEntry = new AppConfigurationEntry(
                krbLoginModuleName, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return new AppConfigurationEntry[] {kerberosKeytabConfigEntry};
    }

    public String getPrincipal() {
        return principal;
    }

    public String getKeytabFile() {
        return keytabFile;
    }

}
