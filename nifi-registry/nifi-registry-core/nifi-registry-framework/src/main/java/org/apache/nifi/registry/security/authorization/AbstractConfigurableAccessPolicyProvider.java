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
package org.apache.nifi.registry.security.authorization;

import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.util.PropertyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractConfigurableAccessPolicyProvider implements ConfigurableAccessPolicyProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConfigurableAccessPolicyProvider.class);

    public static final String PROP_USER_GROUP_PROVIDER = "User Group Provider";

    private UserGroupProvider userGroupProvider;
    private UserGroupProviderLookup userGroupProviderLookup;

    @Override
    public final void initialize(final AccessPolicyProviderInitializationContext initializationContext) throws SecurityProviderCreationException {
        LOGGER.debug("Initializing " + getClass().getCanonicalName());
        userGroupProviderLookup = initializationContext.getUserGroupProviderLookup();
        doInitialize(initializationContext);
        LOGGER.debug("Done initializing " + getClass().getCanonicalName());
    }

    /**
     * Sub-classes can override this method to perform additional initialization.
     */
    protected void doInitialize(final AccessPolicyProviderInitializationContext initializationContext)
            throws SecurityProviderCreationException {

    }

    @Override
    public final void onConfigured(final AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
        try {
            LOGGER.debug("Configuring " + getClass().getCanonicalName());

            final PropertyValue userGroupProviderIdentifier = configurationContext.getProperty(PROP_USER_GROUP_PROVIDER);
            if (!userGroupProviderIdentifier.isSet()) {
                throw new SecurityProviderCreationException("The user group provider must be specified.");
            }

            userGroupProvider = userGroupProviderLookup.getUserGroupProvider(userGroupProviderIdentifier.getValue());
            if (userGroupProvider == null) {
                throw new SecurityProviderCreationException("Unable to locate user group provider with identifier '" + userGroupProviderIdentifier.getValue() + "'");
            }

            doOnConfigured(configurationContext);

            LOGGER.debug("Done configuring " + getClass().getCanonicalName());
        } catch (Exception e) {
            throw new SecurityProviderCreationException(e);
        }
    }

    /**
     * Sub-classes can override this method to perform additional actions during onConfigured.
     */
    protected void doOnConfigured(final AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {

    }

    @Override
    public UserGroupProvider getUserGroupProvider() {
        return userGroupProvider;
    }

}
