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
package org.apache.nifi.kafka.shared.login;

import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.SELF_CONTAINED_KERBEROS_USER_SERVICE;

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.kerberos.SelfContainedKerberosUserService;
import org.apache.nifi.security.krb.KerberosUser;

import javax.security.auth.login.AppConfigurationEntry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Kerberos User Service Login Module implementation of configuration provider
 */
public class KerberosUserServiceLoginConfigProvider implements LoginConfigProvider {
    private static final String SPACE = " ";

    private static final String EQUALS = "=";

    private static final String DOUBLE_QUOTE = "\"";

    private static final String SEMI_COLON = ";";

    private static final Map<AppConfigurationEntry.LoginModuleControlFlag, String> CONTROL_FLAGS = new LinkedHashMap<>();

    static {
        CONTROL_FLAGS.put(AppConfigurationEntry.LoginModuleControlFlag.OPTIONAL, "optional");
        CONTROL_FLAGS.put(AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, "required");
        CONTROL_FLAGS.put(AppConfigurationEntry.LoginModuleControlFlag.REQUISITE, "requisite");
        CONTROL_FLAGS.put(AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT, "sufficient");
    }

    /**
     * Get JAAS configuration using configured Kerberos credentials
     *
     * @param context Property Context
     * @return JAAS configuration with Login Module based on User Service configuration
     */
    @Override
    public String getConfiguration(final PropertyContext context) {
        final KerberosUserService kerberosUserService = context.getProperty(SELF_CONTAINED_KERBEROS_USER_SERVICE).asControllerService(SelfContainedKerberosUserService.class);
        final KerberosUser kerberosUser = kerberosUserService.createKerberosUser();
        final AppConfigurationEntry configurationEntry = kerberosUser.getConfigurationEntry();

        final StringBuilder builder = new StringBuilder();

        final String loginModuleName = configurationEntry.getLoginModuleName();
        builder.append(loginModuleName);

        final AppConfigurationEntry.LoginModuleControlFlag controlFlag = configurationEntry.getControlFlag();
        final String moduleControlFlag = Objects.requireNonNull(CONTROL_FLAGS.get(controlFlag), "Control Flag not found");
        builder.append(SPACE);
        builder.append(moduleControlFlag);

        final Map<String, ?> options = configurationEntry.getOptions();
        options.forEach((key, value) -> {
            builder.append(SPACE);

            builder.append(key);
            builder.append(EQUALS);
            if (value instanceof String) {
                builder.append(DOUBLE_QUOTE);
                builder.append(value);
                builder.append(DOUBLE_QUOTE);
            } else {
                builder.append(value);
            }
        });

        builder.append(SEMI_COLON);
        return builder.toString();
    }
}
