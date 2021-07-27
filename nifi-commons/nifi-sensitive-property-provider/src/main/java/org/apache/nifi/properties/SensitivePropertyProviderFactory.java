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
package org.apache.nifi.properties;

import java.util.Collection;

public interface SensitivePropertyProviderFactory {

    /**
     * Gives the appropriate SensitivePropertyProvider, given a protection scheme.
     * @param protectionScheme The protection scheme to use
     * @return The appropriate SensitivePropertyProvider
     */
    SensitivePropertyProvider getProvider(PropertyProtectionScheme protectionScheme);

    /**
     * Returns a collection of all supported sensitive property providers.
     * @return The supported sensitive property providers
     */
    Collection<SensitivePropertyProvider> getSupportedSensitivePropertyProviders();

    /**
     * Returns a ProtectedPropertyContext with the given property name.  The ProtectedPropertyContext's
     * contextName will be the name found in a matching context mapping from bootstrap.conf, or 'default' if
     * no matching mapping was found.
     * @param groupIdentifier The identifier of a group that contains the configuration property.  The definition
     *                        of a group depends on the type of configuration file.
     * @param propertyName A property name
     * @return The property context, using any mappings configured in bootstrap.conf to match against the
     * provided group identifier (or the default context if none match).
     */
    ProtectedPropertyContext getPropertyContext(String groupIdentifier, String propertyName);
}
