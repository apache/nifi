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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.knox.KnoxAuthenticationFilter;
import org.apache.nifi.web.security.knox.KnoxAuthenticationProvider;
import org.apache.nifi.web.security.knox.KnoxService;
import org.apache.nifi.web.security.knox.KnoxServiceFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;

/**
 * Knox Configuration for Authentication Security
 */
@Configuration
public class KnoxAuthenticationSecurityConfiguration {
    private final NiFiProperties niFiProperties;

    private final Authorizer authorizer;

    @Autowired
    public KnoxAuthenticationSecurityConfiguration(
            final NiFiProperties niFiProperties,
            final Authorizer authorizer
    ) {
        this.niFiProperties = niFiProperties;
        this.authorizer = authorizer;
    }

    @Bean
    public KnoxAuthenticationFilter knoxAuthenticationFilter(final AuthenticationManager authenticationManager) {
        final KnoxAuthenticationFilter knoxAuthenticationFilter = new KnoxAuthenticationFilter();
        knoxAuthenticationFilter.setAuthenticationManager(authenticationManager);
        knoxAuthenticationFilter.setProperties(niFiProperties);
        return knoxAuthenticationFilter;
    }

    @Bean
    public KnoxAuthenticationProvider knoxAuthenticationProvider() {
        return new KnoxAuthenticationProvider(knoxService(), niFiProperties, authorizer);
    }

    @Bean
    public KnoxService knoxService() {
        final KnoxServiceFactoryBean knoxServiceFactoryBean = new KnoxServiceFactoryBean();
        knoxServiceFactoryBean.setProperties(niFiProperties);
        return knoxServiceFactoryBean.getObject();
    }
}
