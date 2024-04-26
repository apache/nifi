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
package org.apache.nifi.web.security.jwt.revocation;

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
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardJwtRevocationServiceTest {
    private static final String ID = UUID.randomUUID().toString();

    private static final Scope SCOPE = Scope.LOCAL;

    private static final Instant EXPIRED = Instant.now().minusSeconds(60);

    @Mock
    private StateManager stateManager;

    @Mock
    private StateMap stateMap;

    @Captor
    private ArgumentCaptor<Map<String, String>> stateCaptor;

    private StandardJwtRevocationService service;

    @BeforeEach
    public void setService() {
        service = new StandardJwtRevocationService(stateManager);
    }

    @Test
    public void testDeleteExpired() throws IOException {
        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);
        when(stateMap.toMap()).thenReturn(Collections.singletonMap(ID, EXPIRED.toString()));

        service.deleteExpired();

        verify(stateManager).setState(stateCaptor.capture(), eq(SCOPE));
        final Map<String, String> stateSaved = stateCaptor.getValue();
        assertTrue(stateSaved.isEmpty(), "Expired Key not deleted");
    }

    @Test
    public void testIsRevokedFound() throws IOException {
        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);
        when(stateMap.toMap()).thenReturn(Collections.singletonMap(ID, EXPIRED.toString()));

        assertTrue(service.isRevoked(ID));
    }

    @Test
    public void testIsRevokedNotFound() throws IOException {
        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);
        when(stateMap.toMap()).thenReturn(Collections.emptyMap());

        assertFalse(service.isRevoked(ID));
    }

    @Test
    public void testSetRevoked() throws IOException {
        when(stateManager.getState(eq(SCOPE))).thenReturn(stateMap);
        when(stateMap.toMap()).thenReturn(Collections.emptyMap());

        final Instant expiration = Instant.now();
        service.setRevoked(ID, expiration);

        verify(stateManager).setState(stateCaptor.capture(), eq(SCOPE));
        final Map<String, String> stateSaved = stateCaptor.getValue();
        final String saved = stateSaved.get(ID);
        assertEquals(expiration.toString(), saved, "Expiration not matched");
    }
}
