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

package org.apache.nifi.components.connector.secrets;

import org.apache.nifi.components.connector.Secret;
import org.apache.nifi.components.connector.SecretReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface SecretsManager {

    void initialize(SecretsManagerInitializationContext initializationContext);

    List<Secret> getAllSecrets();

    Set<SecretProvider> getSecretProviders();

    Optional<Secret> getSecret(SecretReference secretReference);

    Map<SecretReference, Secret> getSecrets(Set<SecretReference> secretReferences);

    /**
     * Resolves the given Secret References, optionally bypassing any cached values. When {@code useCache} is
     * {@code false}, implementations that cache Secret values must fetch fresh values from the underlying
     * providers and refresh their cache with the retrieved values, rather than serving previously cached values.
     *
     * @param secretReferences the Secret References to resolve
     * @param useCache whether cached values may be returned; when {@code false}, values are fetched fresh and the cache is refreshed
     * @return a mapping of each Secret Reference to its resolved Secret, or {@code null} for references that cannot be resolved
     */
    default Map<SecretReference, Secret> getSecrets(final Set<SecretReference> secretReferences, final boolean useCache) {
        return getSecrets(secretReferences);
    }

    /**
     * Invalidates any cached secret data, forcing the next access to fetch fresh data
     * from the underlying secret providers.
     */
    void invalidateCache();

}
