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

public abstract class AbstractSensitivePropertyProvider implements SensitivePropertyProvider {
    private final BootstrapProperties bootstrapProperties;

    public AbstractSensitivePropertyProvider(final BootstrapProperties bootstrapProperties) {
        this.bootstrapProperties = bootstrapProperties;
    }

    protected BootstrapProperties getBootstrapProperties() {
        return bootstrapProperties;
    }

    /**
     * Return the appropriate PropertyProtectionScheme for this provider.
     * @return The PropertyProtectionScheme
     */
    protected abstract PropertyProtectionScheme getProtectionScheme();

    @Override
    public String getName() {
        return getProtectionScheme().getName();
    }

    /**
     * Default implementation to return the protection scheme identifier, with no args to populate the identifier key.
     * Concrete classes may choose to override this in order to fill in the identifier with specific args.
     * @return The identifier key
     */
    @Override
    public String getIdentifierKey() {
        return getProtectionScheme().getIdentifier();
    }
}
