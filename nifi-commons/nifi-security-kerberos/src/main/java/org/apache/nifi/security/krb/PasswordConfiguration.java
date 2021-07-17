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

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;

/**
 * JAAS Configuration to use when logging in with username/password.
 */
public class PasswordConfiguration extends Configuration {

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        HashMap<String, String> options = new HashMap<String, String>();
        options.put("storeKey", "true");
        options.put("refreshKrb5Config", "true");

        final String krbLoginModuleName = ConfigurationUtil.IS_IBM
                ? ConfigurationUtil.IBM_KRB5_LOGIN_MODULE : ConfigurationUtil.SUN_KRB5_LOGIN_MODULE;

        return new AppConfigurationEntry[] {
                new AppConfigurationEntry(
                        krbLoginModuleName,
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options
                )
        };
    }

}
