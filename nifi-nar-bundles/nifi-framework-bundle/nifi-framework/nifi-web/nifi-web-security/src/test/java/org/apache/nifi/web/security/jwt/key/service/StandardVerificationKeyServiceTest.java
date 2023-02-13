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
package org.apache.nifi.web.security.jwt.key.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardVerificationKeyServiceTest {
    private static final String ID = UUID.randomUUID().toString();

    private static final String ALGORITHM = "RSA";

    private static final byte[] ENCODED = ALGORITHM.getBytes(StandardCharsets.UTF_8);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    private static final Scope SCOPE = Scope.LOCAL;

    private static final Instant EXPIRED = Instant.now().minusSeconds(60);

    @Mock
    private StateManager stateManager;

    @Mock
    private StateMap stateMap;

    @Mock
    private Key key;

    @Captor
    private ArgumentCaptor<Map<String, String>> stateCaptor;

    private StandardVerificationKeyService service;

    @BeforeEach
    public void setService() {
        service = new StandardVerificationKeyService(stateManager);
    }

    @Test
    public void testDeleteExpired() throws IOException {
        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);

        final String serialized = getSerializedVerificationKey(EXPIRED);
        when(stateMap.toMap()).thenReturn(Collections.singletonMap(ID, serialized));

        service.deleteExpired();

        verify(stateManager).setState(stateCaptor.capture(), eq(SCOPE));
        final Map<String, String> stateSaved = stateCaptor.getValue();
        assertTrue(stateSaved.isEmpty(), "Expired Key not deleted");
    }

    @Test
    public void testSave() throws IOException {
        when(key.getAlgorithm()).thenReturn(ALGORITHM);
        when(key.getEncoded()).thenReturn(ENCODED);
        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);
        when(stateMap.toMap()).thenReturn(Collections.emptyMap());

        final Instant expiration = Instant.now();
        service.save(ID, key, expiration);

        verify(stateManager).setState(stateCaptor.capture(), eq(SCOPE));
        final Map<String, String> stateSaved = stateCaptor.getValue();
        final String serialized = stateSaved.get(ID);
        assertNotNull(serialized,"Serialized Key not found");
    }

    @Test
    public void testSetExpiration() throws IOException {
        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);
        when(stateMap.toMap()).thenReturn(Collections.emptyMap());

        final Instant expiration = Instant.now();
        final String serialized = getSerializedVerificationKey(expiration);
        when(stateMap.get(eq(ID))).thenReturn(serialized);

        service.setExpiration(ID, expiration);

        verify(stateManager).setState(stateCaptor.capture(), eq(SCOPE));
        final Map<String, String> stateSaved = stateCaptor.getValue();
        final String saved = stateSaved.get(ID);
        assertNotNull(saved, "Serialized Key not found");
    }

    private String getSerializedVerificationKey(final Instant expiration) throws JsonProcessingException {
        final VerificationKey verificationKey = new VerificationKey();
        verificationKey.setId(ID);
        verificationKey.setExpiration(expiration);
        return OBJECT_MAPPER.writeValueAsString(verificationKey);
    }
}
