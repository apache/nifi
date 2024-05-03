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
package org.apache.nifi.web.security.configuration;

import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.kerberos.KerberosService;
import org.apache.nifi.web.security.spring.KerberosServiceFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Kerberos Configuration for Authentication Security
 */
@Configuration
public class KerberosAuthenticationSecurityConfiguration {
    private final NiFiProperties niFiProperties;

    @Autowired
    public KerberosAuthenticationSecurityConfiguration(
            final NiFiProperties niFiProperties
    ) {
        this.niFiProperties = niFiProperties;
    }

    @Bean
    public KerberosService kerberosService() throws Exception {
        final KerberosServiceFactoryBean kerberosServiceFactoryBean = new KerberosServiceFactoryBean();
        kerberosServiceFactoryBean.setProperties(niFiProperties);
        return kerberosServiceFactoryBean.getObject();
    }
}
