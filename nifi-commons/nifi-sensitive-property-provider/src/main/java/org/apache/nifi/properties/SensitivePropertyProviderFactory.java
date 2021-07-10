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
import java.util.Optional;

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
     * Given the &lt;identifier&gt; value providing a group context for an XML configuration file property,
     * returns the custom property context location, if applicable.
     * @param groupIdentifier The value of &lt;identifier&gt; from the enclosing XML block of a property in
     *                        one of the XML configuration files.
     * @return An optional custom property context location, if any mappings are configured in bootstrap.conf
     * and the identifier matches one of the mapping patterns.
     */
    Optional<String> getCustomPropertyContextLocation(String groupIdentifier);
}
