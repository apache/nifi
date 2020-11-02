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
package org.apache.nifi.admin.service;

import org.apache.nifi.idp.IdpCredential;

/**
 * Manages IDP Credentials.
 */
public interface IdpCredentialService {

    /**
     * Creates the given credential.
     *
     * @param credential the credential
     * @return the credential with the id
     */
    IdpCredential createCredential(IdpCredential credential);

    /**
     * Gets the credential for the given identity.
     *
     * @param identity the user identity
     * @return the credential or null if one does not exist for the given identity
     */
    IdpCredential getCredential(String identity);

    /**
     * Deletes the credential with the given id.
     *
     * @param id the credential id
     */
    void deleteCredential(int id);

    /**
     * Replaces the credential for the given user identity.
     *
     * @param credential the new credential
     * @return the credential with the id
     */
    IdpCredential replaceCredential(IdpCredential credential);

}
