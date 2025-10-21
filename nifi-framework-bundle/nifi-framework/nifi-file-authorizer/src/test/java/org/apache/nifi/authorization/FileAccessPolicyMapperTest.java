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
package org.apache.nifi.authorization;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileAccessPolicyMapperTest {

    private static final String USER = "user-identifier";

    private static final String GROUP = "group-identifier";

    private final FileAccessPolicyMapper mapper = new FileAccessPolicyMapper();

    @Test
    void testWriteReadEmpty() {
        final List<AccessPolicy> policies = List.of();

        assertWriteReadEquals(policies);
    }

    @Test
    void testWriteRead() {
        final String identifier = AccessPolicy.class.getName();
        final String resource = AccessPolicy.class.getSimpleName();
        final RequestAction requestAction = RequestAction.READ;

        final AccessPolicy accessPolicy = new AccessPolicy.Builder()
                .identifier(identifier)
                .resource(resource)
                .action(requestAction)
                .addUser(USER)
                .addGroup(GROUP)
                .build();

        final List<AccessPolicy> policies = List.of(accessPolicy);

        final List<AccessPolicy> readPolicies = assertWriteReadEquals(policies);

        final AccessPolicy readPolicy = readPolicies.getFirst();
        assertEquals(identifier, readPolicy.getIdentifier());
        assertEquals(resource, readPolicy.getResource());
        assertEquals(requestAction, readPolicy.getAction());
        assertEquals(USER, readPolicy.getUsers().iterator().next());
        assertEquals(GROUP, readPolicy.getGroups().iterator().next());
    }

    private List<AccessPolicy> assertWriteReadEquals(final List<AccessPolicy> policies) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        mapper.writeAccessPolicies(policies, outputStream);

        final byte[] serialized = outputStream.toByteArray();
        final List<AccessPolicy> read = mapper.readAccessPolicies(new ByteArrayInputStream(serialized));

        assertEquals(policies, read);

        return read;
    }
}
