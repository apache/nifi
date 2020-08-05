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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom JAAS Configuration object for a provided principal that already has a ticket in the ticket cache.
 */
public class TicketCacheConfiguration extends Configuration {

    private static final Logger LOGGER = LoggerFactory.getLogger(TicketCacheConfiguration.class);

    private final String principal;

    private final AppConfigurationEntry ticketCacheConfigEntry;

    public TicketCacheConfiguration(final String principal) {
        if (StringUtils.isBlank(principal)) {
            throw new IllegalArgumentException("Principal cannot be null");
        }

        this.principal = principal;

        final Map<String, String> options = new HashMap<>();
        options.put("principal", principal);
        options.put("refreshKrb5Config", "true");
        options.put("useTicketCache", "true");

        final String krbLoginModuleName = ConfigurationUtil.IS_IBM
                ? ConfigurationUtil.IBM_KRB5_LOGIN_MODULE : ConfigurationUtil.SUN_KRB5_LOGIN_MODULE;

        LOGGER.debug("krbLoginModuleName: {}, configuration options: {}", krbLoginModuleName, options);
        this.ticketCacheConfigEntry = new AppConfigurationEntry(
                krbLoginModuleName, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return new AppConfigurationEntry[] {ticketCacheConfigEntry};
    }

    public String getPrincipal() {
        return principal;
    }

}
