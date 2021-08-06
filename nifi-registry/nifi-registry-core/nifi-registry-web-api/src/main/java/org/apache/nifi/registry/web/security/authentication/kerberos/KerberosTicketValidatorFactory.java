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
import org.springframework.core.io.FileSystemResource;
import org.springframework.security.kerberos.authentication.KerberosTicketValidator;
import org.springframework.security.kerberos.authentication.sun.GlobalSunJaasKerberosConfig;
import org.springframework.security.kerberos.authentication.sun.SunJaasKerberosTicketValidator;

import java.io.File;

@Configuration
public class KerberosTicketValidatorFactory {

    private NiFiRegistryProperties properties;

    private KerberosTicketValidator kerberosTicketValidator;

    @Autowired
    public KerberosTicketValidatorFactory(NiFiRegistryProperties properties) {
        this.properties = properties;
    }

    @Bean
    public KerberosTicketValidator kerberosTicketValidator() throws Exception {

        if (kerberosTicketValidator == null && properties.isKerberosSpnegoSupportEnabled()) {

            // Configure SunJaasKerberos (global)
            final File krb5ConfigFile = properties.getKerberosConfigurationFile();
            if (krb5ConfigFile != null) {
                final GlobalSunJaasKerberosConfig krb5Config = new GlobalSunJaasKerberosConfig();
                krb5Config.setKrbConfLocation(krb5ConfigFile.getAbsolutePath());
                krb5Config.afterPropertiesSet();
            }

            // Create ticket validator to inject into KerberosServiceAuthenticationProvider
            SunJaasKerberosTicketValidator ticketValidator = new SunJaasKerberosTicketValidator();
            ticketValidator.setServicePrincipal(properties.getKerberosSpnegoPrincipal());
            ticketValidator.setKeyTabLocation(new FileSystemResource(properties.getKerberosSpnegoKeytabLocation()));
            ticketValidator.afterPropertiesSet();

            kerberosTicketValidator = ticketValidator;

        }

        return kerberosTicketValidator;

    }

}
