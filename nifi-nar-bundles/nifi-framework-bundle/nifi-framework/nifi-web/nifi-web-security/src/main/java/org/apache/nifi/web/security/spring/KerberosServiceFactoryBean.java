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
package org.apache.nifi.web.security.spring;

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.kerberos.AlternateKerberosUserDetailsService;
import org.apache.nifi.web.security.kerberos.KerberosService;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.security.kerberos.authentication.KerberosServiceAuthenticationProvider;
import org.springframework.security.kerberos.authentication.KerberosTicketValidator;
import org.springframework.security.kerberos.authentication.sun.GlobalSunJaasKerberosConfig;
import org.springframework.security.kerberos.authentication.sun.SunJaasKerberosTicketValidator;

import java.io.File;

public class KerberosServiceFactoryBean implements FactoryBean<KerberosService> {

    private KerberosService kerberosService = null;
    private NiFiProperties properties = null;

    @Override
    public KerberosService getObject() throws Exception {
        if (kerberosService == null && properties.isKerberosSpnegoSupportEnabled()) {
            final File krb5ConfigFile = properties.getKerberosConfigurationFile();
            if (krb5ConfigFile != null) {
                final GlobalSunJaasKerberosConfig krb5Config = new GlobalSunJaasKerberosConfig();
                krb5Config.setKrbConfLocation(krb5ConfigFile.getAbsolutePath());
                krb5Config.afterPropertiesSet();
            }

            kerberosService = new KerberosService();
            kerberosService.setKerberosServiceAuthenticationProvider(createKerberosServiceAuthenticationProvider());
        }

        return kerberosService;
    }

    @Override
    public Class<?> getObjectType() {
        return KerberosService.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    private KerberosServiceAuthenticationProvider createKerberosServiceAuthenticationProvider() throws Exception {
        KerberosServiceAuthenticationProvider kerberosServiceAuthenticationProvider = new KerberosServiceAuthenticationProvider();
        kerberosServiceAuthenticationProvider.setTicketValidator(createTicketValidator());
        kerberosServiceAuthenticationProvider.setUserDetailsService(createAlternateKerberosUserDetailsService());
        kerberosServiceAuthenticationProvider.afterPropertiesSet();
        return kerberosServiceAuthenticationProvider;
    }

    private AlternateKerberosUserDetailsService createAlternateKerberosUserDetailsService() {
        return new AlternateKerberosUserDetailsService();
    }

    private KerberosTicketValidator createTicketValidator() throws Exception {
        SunJaasKerberosTicketValidator ticketValidator = new SunJaasKerberosTicketValidator();
        ticketValidator.setServicePrincipal(properties.getKerberosSpnegoPrincipal());
        ticketValidator.setKeyTabLocation(new FileSystemResource(properties.getKerberosSpnegoKeytabLocation()));
        ticketValidator.afterPropertiesSet();
        return ticketValidator;
    }
}
