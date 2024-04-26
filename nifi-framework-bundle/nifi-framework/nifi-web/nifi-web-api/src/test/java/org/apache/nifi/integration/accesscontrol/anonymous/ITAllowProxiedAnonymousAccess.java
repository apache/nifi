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
package org.apache.nifi.integration.accesscontrol.anonymous;

import org.apache.nifi.integration.accesscontrol.AccessControlHelper;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import jakarta.ws.rs.core.Response;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration test for allowing proxied anonymous access.
 */
public class ITAllowProxiedAnonymousAccess extends AbstractAnonymousUserTest {

    private static AccessControlHelper helper;

    @BeforeAll
    public static void setup() throws Exception {
        helper = new AccessControlHelper("src/test/resources/access-control/nifi-anonymous-allowed.properties");
    }

    /**
     * Attempt to create a processor anonymously.
     *
     * @throws Exception ex
     */
    @Test
    public void testProxiedAnonymousAccess() throws Exception {
        final Response response = super.testCreateProcessor(helper.getBaseUrl(), helper.getAnonymousUser());

        // ensure the request is successful
        assertEquals(201, response.getStatus());

        // get the entity body
        final ProcessorEntity entity = response.readEntity(ProcessorEntity.class);

        // verify creation
        final ProcessorDTO processor = entity.getComponent();
        assertEquals("Copy", processor.getName());
        assertEquals("org.apache.nifi.integration.util.SourceTestProcessor", processor.getType());
    }

    @AfterAll
    public static void cleanup() throws Exception {
        helper.cleanup();
    }
}
