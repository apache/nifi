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
package org.apache.nifi.web.security.saml.impl;

import org.apache.nifi.admin.service.IdpCredentialService;
import org.apache.nifi.idp.IdpCredential;
import org.apache.nifi.idp.IdpType;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.web.security.saml.SAMLCredentialStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.saml.SAMLCredential;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Standard implementation of SAMLCredentialStore that uses Java serialization to store
 * SAMLCredential objects as BLOBs in a relational database.
 */
public class StandardSAMLCredentialStore implements SAMLCredentialStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardSAMLCredentialStore.class);

    private final IdpCredentialService idpCredentialService;

    public StandardSAMLCredentialStore(final IdpCredentialService idpCredentialService) {
        this.idpCredentialService = idpCredentialService;
    }

    @Override
    public void save(final String identity, final SAMLCredential credential) {
        if (StringUtils.isBlank(identity)) {
            throw new IllegalArgumentException("Identity cannot be null");
        }

        if (credential == null) {
            throw new IllegalArgumentException("Credential cannot be null");
        }

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final ObjectOutputStream objOut = new ObjectOutputStream(baos)) {

            objOut.writeObject(credential);
            objOut.flush();
            baos.flush();

            final IdpCredential idpCredential = new IdpCredential();
            idpCredential.setIdentity(identity);
            idpCredential.setType(IdpType.SAML);
            idpCredential.setCredential(baos.toByteArray());

            // replace issues a delete first in case the user already has a stored credential that wasn't properly cleaned up on logout
            final IdpCredential createdIdpCredential = idpCredentialService.replaceCredential(idpCredential);
            LOGGER.debug("Successfully saved SAMLCredential for {} with id {}", identity, createdIdpCredential.getId());

        } catch (IOException e) {
            throw new RuntimeException("Unable to serialize SAMLCredential for user with identity " + identity, e);
        }
    }

    @Override
    public SAMLCredential get(final String identity) {
        final IdpCredential idpCredential = idpCredentialService.getCredential(identity);
        if (idpCredential == null) {
            LOGGER.debug("No SAMLCredential exists for {}", identity);
            return null;
        }

        final IdpType idpType = idpCredential.getType();
        if (idpType != IdpType.SAML) {
            LOGGER.debug("Stored credential for {} was not a SAML credential, type was {}", identity, idpType);
            return null;
        }

        final byte[] serializedCredential = idpCredential.getCredential();

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serializedCredential);
             final ObjectInputStream objIn = new ObjectInputStream(bais)) {

            final SAMLCredential samlCredential = (SAMLCredential) objIn.readObject();
            return samlCredential;

        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Unable to deserialize SAMLCredential for user with identity " + identity, e);
        }
    }

    @Override
    public void delete(final String identity) {
        final IdpCredential idpCredential = idpCredentialService.getCredential(identity);

        if (idpCredential == null) {
            LOGGER.debug("No SAMLCredential exists for {}", identity);
            return;
        }

        final IdpType idpType = idpCredential.getType();
        if (idpType != IdpType.SAML) {
            LOGGER.debug("Stored credential for {} was not a SAML credential, type was {}", identity, idpType);
            return;
        }

        idpCredentialService.deleteCredential(idpCredential.getId());
    }

}
