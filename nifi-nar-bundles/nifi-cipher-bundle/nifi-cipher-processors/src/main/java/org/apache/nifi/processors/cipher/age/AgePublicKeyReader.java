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

import com.exceptionfactory.jagged.RecipientStanzaWriter;
import com.exceptionfactory.jagged.x25519.X25519RecipientStanzaWriterFactory;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Provider;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * X25519 Public Key implementation age Key Reader
 */
public class AgePublicKeyReader extends AbstractAgeKeyReader<RecipientStanzaWriter> {
    private static final Provider KEY_PROVIDER = AgeProviderResolver.getKeyProvider().orElse(null);

    public AgePublicKeyReader() {
        super(AgeKeyIndicator.PUBLIC_KEY);
    }

    /**
     * Read public keys and return Recipient Stanza Writers
     *
     * @param keys Set of public keys
     * @return Recipient Stanza Writers
     * @throws IOException Thrown on failure to parse public keys
     */
    @Override
    protected List<RecipientStanzaWriter> readKeys(final Set<String> keys) throws IOException {
        final List<RecipientStanzaWriter> recipientStanzaWriters = new ArrayList<>();
        for (final String encodedPublicKey : keys) {
            try {
                final RecipientStanzaWriter recipientStanzaWriter = getRecipientStanzaWriter(encodedPublicKey);
                recipientStanzaWriters.add(recipientStanzaWriter);
            } catch (final Exception e) {
                throw new IOException("Parsing Public Key Recipients failed", e);
            }
        }

        return recipientStanzaWriters;
    }

    private RecipientStanzaWriter getRecipientStanzaWriter(final String encodedPublicKey) throws GeneralSecurityException {
        final RecipientStanzaWriter recipientStanzaWriter;

        if (KEY_PROVIDER == null) {
            recipientStanzaWriter = X25519RecipientStanzaWriterFactory.newRecipientStanzaWriter(encodedPublicKey);
        } else {
            recipientStanzaWriter = X25519RecipientStanzaWriterFactory.newRecipientStanzaWriter(encodedPublicKey, KEY_PROVIDER);
        }

        return recipientStanzaWriter;
    }
}
