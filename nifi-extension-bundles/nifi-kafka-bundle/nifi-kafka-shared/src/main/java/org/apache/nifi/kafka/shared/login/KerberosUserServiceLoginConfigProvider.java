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

import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.kerberos.SelfContainedKerberosUserService;
import org.apache.nifi.security.krb.KerberosUser;

import javax.security.auth.login.AppConfigurationEntry;
import java.util.Map;

import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.SELF_CONTAINED_KERBEROS_USER_SERVICE;

/**
 * Kerberos User Service Login Module implementation of configuration provider
 */
public class KerberosUserServiceLoginConfigProvider implements LoginConfigProvider {

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

        final LoginConfigBuilder builder = new LoginConfigBuilder(configurationEntry.getLoginModuleName(), configurationEntry.getControlFlag());

        final Map<String, ?> options = configurationEntry.getOptions();
        options.forEach(builder::append);

        return builder.build();
    }
}
