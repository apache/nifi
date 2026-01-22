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

/**
 * Provides information about a secret provider (source) that is aggregated
 * by a {@link SecretsProvider}.
 */
public interface SecretProviderInfo {

    /**
     * Returns the unique identifier of the secret provider.
     *
     * @return the provider identifier
     */
    String getIdentifier();

    /**
     * Returns the display name of the secret provider.
     *
     * @return the provider name
     */
    String getName();

    /**
     * Returns the type of the secret provider (e.g., "HashiCorp Vault", "AWS Secrets Manager").
     *
     * @return the provider type
     */
    String getType();

    /**
     * Returns a description of the secret provider.
     *
     * @return the provider description, or null if not available
     */
    String getDescription();
}
