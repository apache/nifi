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
package org.apache.nifi.web.security.knox;

import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.factory.FactoryBean;

public class KnoxServiceFactoryBean implements FactoryBean<KnoxService> {

    private KnoxService knoxService = null;
    private NiFiProperties properties = null;

    @Override
    public KnoxService getObject() throws Exception {
        if (knoxService == null) {
            // ensure we only allow knox if login and oidc are disabled
            if (properties.isKnoxSsoEnabled() && (properties.isLoginIdentityProviderEnabled() || properties.isOidcEnabled())) {
                throw new RuntimeException("Apache Knox SSO support cannot be enabled if the Login Identity Provider or OpenId Connect is configured.");
            }

            final KnoxConfiguration configuration = new StandardKnoxConfiguration(properties);
            knoxService = new KnoxService(configuration);
        }

        return knoxService;
    }

    @Override
    public Class<?> getObjectType() {
        return KnoxService.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

}
