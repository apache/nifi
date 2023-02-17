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
package org.apache.nifi.web.security.jwt.jws;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StandardJwsSignerProviderTest {
    private static final String KEY_IDENTIFIER = UUID.randomUUID().toString();

    @Mock
    private SigningKeyListener signingKeyListener;

    @Mock
    private JwsSignerContainer jwsSignerContainer;

    @Captor
    private ArgumentCaptor<String> keyIdentifierCaptor;

    @Captor
    private ArgumentCaptor<Instant> expirationCaptor;

    private StandardJwsSignerProvider provider;

    @Before
    public void setProvider() {
        provider = new StandardJwsSignerProvider(signingKeyListener);
        when(jwsSignerContainer.getKeyIdentifier()).thenReturn(KEY_IDENTIFIER);
    }

    @Test
    public void testOnSignerUpdated() {
        provider.onSignerUpdated(jwsSignerContainer);
        final Instant expiration = Instant.now();
        final JwsSignerContainer container = provider.getJwsSignerContainer(expiration);

        assertEquals("JWS Signer Container not matched", jwsSignerContainer, container);

        verify(signingKeyListener).onSigningKeyUsed(keyIdentifierCaptor.capture(), expirationCaptor.capture());
        assertEquals(KEY_IDENTIFIER, keyIdentifierCaptor.getValue());
        assertEquals(expiration, expirationCaptor.getValue());
    }
}
