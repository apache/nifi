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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * <p>
 * Provides access to secrets for use in connector configuration. Implementations of this
 * interface are responsible for retrieving secrets from external secret management systems
 * such as HashiCorp Vault, AWS Secrets Manager, Azure Key Vault, or other secret stores.
 * </p>
 *
 * <p>
 * The default implementation may aggregate secrets from multiple underlying secret providers.
 * Third-party implementations may provide access to custom secret management systems.
 * </p>
 *
 * <p>
 * Implementations must be thread-safe.
 * </p>
 */
public interface SecretsProvider {

    /**
     * Initializes the secrets provider with the given context. This method is called once
     * before any other methods are called.
     *
     * @param context the initialization context providing configuration and dependencies
     * @throws IOException if an I/O error occurs during initialization
     */
    void initialize(SecretsProviderContext context) throws IOException;

    /**
     * Shuts down the secrets provider and releases any resources. After this method is called,
     * the provider may be initialized again via {@link #initialize(SecretsProviderContext)}.
     */
    void shutdown();

    /**
     * Returns all secrets available from this provider. Note that this may be an expensive
     * operation depending on the underlying secret management system.
     *
     * @return a list of all available secrets
     */
    List<Secret> getAllSecrets();

    /**
     * Returns information about the secret providers (sources) that are aggregated
     * by this secrets provider. This is useful for displaying available secret
     * sources in the user interface.
     *
     * @return the set of secret provider information
     */
    Set<SecretProviderInfo> getSecretProviderInfos();

    /**
     * Retrieves a specific secret by its reference.
     *
     * @param secretReference the reference to the secret to retrieve
     * @return an Optional containing the secret if found, or empty if not found
     */
    Optional<Secret> getSecret(SecretReference secretReference);

    /**
     * Retrieves multiple secrets by their references. This method may be more efficient
     * than calling {@link #getSecret(SecretReference)} multiple times when retrieving
     * multiple secrets.
     *
     * @param secretReferences the set of references to the secrets to retrieve
     * @return a map from secret reference to the corresponding secret (missing secrets
     *         will not have entries in the map)
     */
    Map<SecretReference, Secret> getSecrets(Set<SecretReference> secretReferences);

    /**
     * Refreshes the cached secrets from the underlying secret management system.
     * This method may be called periodically to ensure secrets are up to date.
     * The default implementation does nothing.
     *
     * @throws IOException if an I/O error occurs during refresh
     */
    default void refresh() throws IOException {
        // Default implementation does nothing
    }
}
