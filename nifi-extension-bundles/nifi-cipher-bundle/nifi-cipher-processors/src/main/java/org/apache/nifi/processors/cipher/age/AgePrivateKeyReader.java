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
package org.apache.nifi.processors.cipher.age;

import com.exceptionfactory.jagged.RecipientStanzaReader;
import com.exceptionfactory.jagged.x25519.X25519RecipientStanzaReaderFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Provider;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * X25519 Private Key implementation age Key Reader
 */
public class AgePrivateKeyReader extends AbstractAgeKeyReader<RecipientStanzaReader> {
    private static final Provider KEY_PROVIDER = AgeProviderResolver.getKeyProvider().orElse(null);

    public AgePrivateKeyReader() {
        super(AgeKeyIndicator.PRIVATE_KEY);
    }

    /**
     * Read private keys and return Recipient Stanza Writers
     *
     * @param keys Set of private keys
     * @return Recipient Stanza Writers
     * @throws IOException Thrown on failures parsing private keys
     */
    @Override
    protected List<RecipientStanzaReader> readKeys(final Set<String> keys) throws IOException {
        final List<RecipientStanzaReader> recipientStanzaReaders = new ArrayList<>();
        for (final String encodedPrivateKey : keys) {
            try {
                final RecipientStanzaReader recipientStanzaReader = getRecipientStanzaReader(encodedPrivateKey);
                recipientStanzaReaders.add(recipientStanzaReader);
            } catch (final Exception e) {
                throw new IOException("Parsing Private Key Identities failed", e);
            }
        }

        return recipientStanzaReaders;
    }

    private RecipientStanzaReader getRecipientStanzaReader(final String encodedPrivateKey) throws GeneralSecurityException {
        final RecipientStanzaReader recipientStanzaReader;

        if (KEY_PROVIDER == null) {
            recipientStanzaReader = X25519RecipientStanzaReaderFactory.newRecipientStanzaReader(encodedPrivateKey);
        } else {
            recipientStanzaReader = X25519RecipientStanzaReaderFactory.newRecipientStanzaReader(encodedPrivateKey, KEY_PROVIDER);
        }

        return recipientStanzaReader;
    }
}
