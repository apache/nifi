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
package org.apache.nifi.registry.web.security.authentication.kerberos;

import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.kerberos.authentication.KerberosServiceAuthenticationProvider;
import org.springframework.security.kerberos.authentication.KerberosTicketValidator;

@Configuration
public class KerberosSpnegoFactory {

    @Autowired
    private NiFiRegistryProperties properties;

    @Autowired(required = false)
    private KerberosTicketValidator kerberosTicketValidator;

    private KerberosServiceAuthenticationProvider kerberosServiceAuthenticationProvider;
    private KerberosSpnegoIdentityProvider kerberosSpnegoIdentityProvider;

    @Bean
    public KerberosSpnegoIdentityProvider kerberosSpnegoIdentityProvider() throws Exception {

        if (kerberosSpnegoIdentityProvider == null && properties.isKerberosSpnegoSupportEnabled()) {
            kerberosSpnegoIdentityProvider = new KerberosSpnegoIdentityProvider(
                    kerberosServiceAuthenticationProvider(),
                    properties);
        }

        return kerberosSpnegoIdentityProvider;
    }


    private KerberosServiceAuthenticationProvider kerberosServiceAuthenticationProvider() throws Exception {

        if (kerberosServiceAuthenticationProvider == null && properties.isKerberosSpnegoSupportEnabled()) {

            KerberosServiceAuthenticationProvider ksap = new KerberosServiceAuthenticationProvider();
            ksap.setTicketValidator(kerberosTicketValidator);
            ksap.setUserDetailsService(new KerberosUserDetailsService());
            ksap.afterPropertiesSet();

            kerberosServiceAuthenticationProvider = ksap;

        }

        return kerberosServiceAuthenticationProvider;
    }

}
